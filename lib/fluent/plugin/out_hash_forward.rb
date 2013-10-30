class Fluent::HashForwardOutput < Fluent::Output
  Fluent::Plugin.register_output('hash_forward', self)

  config_param :remove_prefix, :string, :default => nil
  config_param :add_prefix, :string, :default => nil
  config_param :hash_key, :string, :default => nil

  def configure(conf)
    super

    if @remove_prefix
      @removed_prefix_string = @remove_prefix + '.'
      @removed_length = @removed_prefix_string.length
    end
    if @add_prefix
      @added_prefix_string = @add_prefix + '.'
    end

    @servers = []
    @forward_elements = []
    conf.elements.each {|element|
      if element.name == "server"
        element["weight"] = 100
        @servers.push(element)
      else
        @forward_elements.push(element)
      end
    }
    conf.elements.clear

    @forward_conf = {}
    conf.each {|k, v|
      if !self.class.config_params.keys.index(k.to_sym) and k != "type"
        @forward_conf[k] = v
        conf.delete(k)
      end
    }

    @forward = @servers.map {|server|
      elements = @forward_elements + [server]
      plant(@forward_conf, elements)
    }

    self
  end

  def shutdown
    super
    @forward.each do |output|
      output.shutdown
    end
  end

  def spec(conf, elements)
    Fluent::Config::Element.new('instance', '', conf, elements)
  end

  def plant(conf, elements)
    output = nil
    server = elements.last["host"]+":"+elements.last["port"]
    begin
      output = Fluent::Plugin.new_output('forward')
      output.configure(spec(conf, elements))
      output.start
      $log.info "out_hash_forward plants new output: for server '#{server}'"
    rescue Fluent::ConfigError => e
      $log.error "failed to configure sub output: #{e.message}"
      $log.error e.backtrace.join("\n")
      $log.error "Cannot output messages with server '#{server}'"
      output = nil
    rescue StandardError => e
      $log.error "failed to configure/start sub output: #{e.message}"
      $log.error e.backtrace.join("\n")
      $log.error "Cannot output messages with server '#{server}'"
      output = nil
    end
    output
  end

  def emit(tag, es, chain)
    if @remove_prefix
      if (tag.start_with?(@removed_prefix_string) and tag.length > @removed_length) or tag == @remove_prefix
        tag = tag[@removed_length..-1]
      end
    end 
    if @add_prefix
      tag = (tag.length > 0 ? @added_prefix_string + tag : @add_prefix)
    end

    hash_key = @hash_key ? expand_placeholder(@hash_key, tag) : tag
    index = server_index(hash_key)
    output = @forward[index]
    if output
      output.emit(tag, es, chain)
    else
      chain.next
    end
  end

  def server_index(key)
    require 'murmurhash3'
    MurmurHash3::V32.str_hash(key) % @servers.size
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
