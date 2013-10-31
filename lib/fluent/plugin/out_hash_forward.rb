require 'fluent/plugin/out_forward'

class Fluent::HashForwardOutput < Fluent::ForwardOutput
  Fluent::Plugin.register_output('hash_forward', self)

  config_param :hash_key, :string, :default => nil

  def configure(conf)
    super
    @standby_nodes, @regular_nodes = @nodes.partition {|n| n.standby? }
  end

  attr_reader :regular_nodes
  attr_reader :standby_nodes

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

  # Override: I don't use weight
  def rebuild_weight_array
  end

  # Get nodes (a regular_node and a standby_node if available) using hash algorithm
  def nodes(tag)
    hash_key = @hash_key ? expand_placeholder(@hash_key, tag) : tag
    regular_index = get_index(hash_key, regular_nodes.size)
    standby_index = standby_nodes.size > 0 ? get_index(hash_key, standby_nodes.size) : 0
    [regular_nodes[regular_index], standby_nodes[standby_index]].compact
  end

  # hashing(key) mod N
  def get_index(key, size)
    require 'murmurhash3'
    MurmurHash3::V32.str_hash(key) % size
  end

  # Replace ${tag} and ${tags} placeholders in a string
  #
  # @param [String] str    the string to be expanded
  # @param [String] tag    tag of a message
  def expand_placeholder(str, tag)
    struct = UndefOpenStruct.new
    struct.tag  = tag
    struct.tags = tag.split('.')
    str = str.gsub(/\$\{([^}]+)\}/, '#{\1}') # ${..} => #{..}
    eval "\"#{str}\"", struct.instance_eval { binding }
  end

  class UndefOpenStruct < OpenStruct
    (Object.instance_methods).each do |m|
      undef_method m unless m.to_s =~ /^__|respond_to_missing\?|object_id|public_methods|instance_eval|method_missing|define_singleton_method|respond_to\?|new_ostruct_member/
    end
  end
end
