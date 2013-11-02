require 'fluent/plugin/out_forward'

class Fluent::HashForwardOutput < Fluent::ForwardOutput
  Fluent::Plugin.register_output('hash_forward', self)

  config_param :hash_key_slice, :string, :default => nil

  def configure(conf)
    super
    if @hash_key_slice
      lindex, rindex = @hash_key_slice.split('..', 2)
      if lindex.nil? or rindex.nil? or lindex !~ /^-?\d+$/ or rindex !~ /^-?\d+$/
        raise Fluent::ConfigError, "out_hash_forard: hash_key_slice must be formatted like [num]..[num]"
      else
        @hash_key_slice_lindex = lindex.to_i
        @hash_key_slice_rindex = rindex.to_i
      end
    end

    @standby_nodes, @regular_nodes = @nodes.partition {|n| n.standby? }
    @regular_weight_array = build_weight_array(@regular_nodes)
    @standby_weight_array = build_weight_array(@standby_nodes)

    @cache_nodes = {}
  end

  # for test
  attr_reader :regular_nodes
  attr_reader :standby_nodes
  attr_reader :regular_weight_array
  attr_reader :standby_weight_array
  attr_accessor :hash_key_slice_lindex
  attr_accessor :hash_key_slice_rindex

  # Override
  def write_objects(tag, chunk)
    return if chunk.empty?

    error = nil

    nodes = nodes(tag)

    # below is just copy from out_forward
    nodes.each do |node|
      if node.available?
        begin
          send_data(node, tag, chunk)
          return
        rescue
          # for load balancing during detecting crashed servers
          error = $!  # use the latest error
        end
      end
    end

    if error
      raise error
    else
      raise "no nodes are available"  # TODO message
    end
  end

  # Override: I change weight algorithm
  def rebuild_weight_array
  end

  def build_weight_array(nodes)
    # below is just a partial copy from out_forward#rebuild_weight_array
    weight_array = []
    gcd = nodes.map {|n| n.weight }.inject(0) {|r,w| r.gcd(w) }
    nodes.each {|n|
      (n.weight / gcd).times {
        weight_array << n
      }
    }
    weight_array
  end

  # Get nodes (a regular_node and a standby_node if available) using hash algorithm
  def nodes(tag)
    if nodes = @cache_nodes[tag]
      return nodes
    end
    hash_key = @hash_key_slice ? perform_hash_key_slice(tag) : tag
    regular_index = @regular_weight_array.size > 0 ? get_index(hash_key, @regular_weight_array.size) : 0
    standby_index = @standby_weight_array.size > 0 ? get_index(hash_key, @standby_weight_array.size) : 0
    nodes = [@regular_weight_array[regular_index], @standby_weight_array[standby_index]].compact
    @cache_nodes[tag] = nodes
  end

  # hashing(key) mod N
  def get_index(key, size)
    str_hash(key) % size
  end

  # the simplest hashing ever
  # https://gist.github.com/sonots/7263495
  def str_hash(key)
    key.bytes.inject(&:+)
  end

  def perform_hash_key_slice(tag)
    tags = tag.split('.')
    sliced = tags[@hash_key_slice_lindex..@hash_key_slice_rindex]
    return sliced.nil? ? "" : sliced.join('.')
  end
end
