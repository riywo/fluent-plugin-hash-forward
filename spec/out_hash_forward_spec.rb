# encoding: UTF-8
require_relative 'spec_helper'

describe Fluent::HashForwardOutput do
  before { Fluent::Test.setup }
  CONFIG = %[
    type hash_forward
    flush_interval 1s

    <server>
      host 192.168.1.3
      port 24224
    </server>
    <server>
      host 192.168.1.4
      port 24224
    </server>

    <secondary>
      type file
      path /var/log/fluent/forward-failed
    </secondary>
  ]
  let(:driver) { Fluent::Test::OutputTestDriver.new(Fluent::HashForwardOutput, tag).configure(config).instance }

  describe 'test configure' do
    let(:tag) { 'test.tag' }
    let(:config) { CONFIG }
    context 'default behavior' do
      it { lambda{ driver }.should_not raise_error }
    end

    describe 'bad hash_key_slice' do
     context 'string' do
       let(:config) { CONFIG + %[hash_key_slice a..b] }
       it { lambda{ driver }.should raise_error(Fluent::ConfigError) }
     end

     context 'no rindex' do
       let(:config) { CONFIG + %[hash_key_slice 0..] }
       it { lambda{ driver }.should raise_error(Fluent::ConfigError) }
     end

     context 'no lindex' do
       let(:config) { CONFIG + %[hash_key_slice ..1] }
       it { lambda{ driver }.should raise_error(Fluent::ConfigError) }
     end

     context 'bad format' do
       let(:config) { CONFIG + %[hash_key_slice 0,1] }
       it { lambda{ driver }.should raise_error(Fluent::ConfigError) }
     end
    end
  end

  describe 'test perform_hash_key_slice' do
    # actually the same behavior with ruby Array, so just conforming ruby Array#slice
    let(:tag) { 'tag0.tag1' }
    context 'larger than tags size' do
      let(:config) { CONFIG + %[hash_key_slice 1..10] }
      it { expect(driver.perform_hash_key_slice(tag)).to eq('tag1') }
    end

    context 'rindex is smaller than lindex' do
      let(:config) { CONFIG + %[hash_key_slice 1..0] }
      it { expect(driver.perform_hash_key_slice(tag)).to eq('') }
    end

    context 'rindex is -1' do
      let(:config) { CONFIG + %[hash_key_slice 0..-1] }
      it { expect(driver.perform_hash_key_slice(tag)).to eq(tag) }
    end

    context 'rindex is -2' do
      let(:config) { CONFIG + %[hash_key_slice 0..-2] }
      it { expect(driver.perform_hash_key_slice(tag)).to eq('tag0') }
    end

    context 'rindex is large negative integer' do
      let(:config) { CONFIG + %[hash_key_slice 0..-10] }
      it { expect(driver.perform_hash_key_slice(tag)).to eq('') }
    end

    context 'lindex is -1' do
      let(:config) { CONFIG + %[hash_key_slice -1..10] }
      it { expect(driver.perform_hash_key_slice(tag)).to eq('tag1') }
    end

    context 'lindex is -2' do
      let(:config) { CONFIG + %[hash_key_slice -2..10] }
      it { expect(driver.perform_hash_key_slice(tag)).to eq(tag) }
    end

    context 'lindex is large negatize integer' do
      # this behavior looks wierd for me
      let(:config) { CONFIG + %[hash_key_slice -3..10] }
      it { expect(driver.perform_hash_key_slice(tag)).to eq('') }
    end
  end

  describe 'test build_weight_array' do
    let(:tag) { 'test.tag' }
    context 'default behavior (60:60 equals to 1:1)' do
      let(:config) { CONFIG }
      it do
        expect(driver.regular_weight_array.size).to eq(driver.regular_nodes.size)
        expect(driver.regular_weight_array[0]).to eq(driver.regular_nodes[0])
        expect(driver.regular_weight_array[1]).to eq(driver.regular_nodes[1])
      end
    end

    context '100:0 equals to 1:0' do
      let(:config) {
        %[
          type hash_forward

          <server>
            host 192.168.1.3
            port 24224
            weight 100
          </server>
          <server>
            host 192.168.1.4
            port 24224
            weight 0
          </server>
        ]
      }
      it do
        expect(driver.regular_weight_array.size).to eq(1)
        expect(driver.regular_weight_array.first).to eq(driver.regular_nodes.first)
      end
    end

    context '100:50 equals to 2:1' do
      let(:config) {
        %[
          type hash_forward

          <server>
            host 192.168.1.3
            port 24224
            weight 100
          </server>
          <server>
            host 192.168.1.4
            port 24224
            weight 50
          </server>
        ]
      }
      it do
        expect(driver.regular_weight_array.size).to eq(3)
        expect(driver.regular_weight_array[0]).to eq(driver.regular_nodes[0])
        expect(driver.regular_weight_array[1]).to eq(driver.regular_nodes[0])
        expect(driver.regular_weight_array[2]).to eq(driver.regular_nodes[1])
      end
    end
  end

  describe 'test hashing' do
    let(:tag) { 'test.tag' }
    let(:config) { CONFIG }

    context 'test consistency' do
      before do
        @node = driver.nodes(tag).first
      end
      it 'should forward to the same node' do
        expect(driver.nodes(tag).first).to eq(@node)
      end
    end

    context 'test distribution' do
      let(:tag1) { 'test.tag1' }
      let(:tag2) { 'test.tag2' }
      before do
        driver.stub(:str_hash).with(tag1).and_return(0)
        driver.stub(:str_hash).with(tag2).and_return(1)
        @node1 = driver.nodes(tag1).first
      end
      it 'should forward to the different node' do
        expect(driver.nodes(tag2).first).not_to eq(@node1)
      end
    end

    context 'test hash_key_slice' do
      let(:tag1) { 'test.tag1' }
      let(:tag2) { 'test.tag2' }
      let(:config) { CONFIG + %[hash_key_slice 0..-2] }
      before do
        @node = driver.nodes(tag1).first
      end
      it 'should forward to the same node' do
        expect(driver.nodes(tag2).first).to eq(@node)
      end
    end
  end

  describe 'test emit' do
    let(:tag) { 'test.tag' }
    let(:time) { Time.now.to_i }
    let(:es) { Array.new(1) }
    let(:chain) { Fluent::NullOutputChain.instance }
    let(:config) { CONFIG }

    context 'default behavior' do
      before do
        Fluent::Engine.stub(:now).and_return(time)
        node = driver.nodes(tag).first
        driver.should_receive(:send_data).with(node, tag, es)
      end
      it 'should forward' do
        driver.write_objects(tag, es)
      end
    end

    context 'test standby' do
      let(:config) {
        %[
          type hash_forward

          <server>
            host 192.168.1.3
            port 24224
          </server>
          <server>
            host 192.168.1.4
            port 24224
            standby true
          </server>
        ]
      }
      before do
        Fluent::Engine.stub(:now).and_return(time)
        regular_node = driver.regular_nodes.first
        standby_node = driver.standby_nodes.first
        regular_node.stub(:available?).and_return(false) # stub as regular node is not available
        driver.should_receive(:send_data).with(standby_node, tag, es)
      end
      it 'should forward to the standby node if regular node is not available' do
        driver.write_objects(tag, es)
      end
    end

    context 'test weight' do
      let(:tag0) { 'test.tag0' }
      let(:tag1) { 'test.tag1' }
      let(:tag2) { 'test.tag2' }
      before do
        Fluent::Engine.stub(:now).and_return(time)
        driver.stub(:get_index).with(tag0, 3).and_return(0)
        driver.stub(:get_index).with(tag1, 3).and_return(1)
        driver.stub(:get_index).with(tag2, 3).and_return(2)
      end

      WEIGHT_CONFIG = %[
        type hash_forward

        <server>
          host 192.168.1.3
          port 24224
          weight 100
        </server>
        <server>
          host 192.168.1.4
          port 24224
          weight 50
        </server>
      ]

      context 'test weight on regular nodes' do
        let(:config) { WEIGHT_CONFIG }
        before do
          driver.should_receive(:send_data).with(driver.regular_nodes[0], tag0, es)
          driver.should_receive(:send_data).with(driver.regular_nodes[0], tag1, es)
          driver.should_receive(:send_data).with(driver.regular_nodes[1], tag2, es)
        end
        it 'should forward to regular nodes considering weights' do
          driver.write_objects(tag0, es)
          driver.write_objects(tag1, es)
          driver.write_objects(tag2, es)
        end
      end

      context 'test weight on standby nodes' do
        let(:config) { WEIGHT_CONFIG + %[
          <server>
            host 192.168.1.3
            port 24224
            weight 100
            standby true
          </server>
          <server>
            host 192.168.1.4
            port 24224
            weight 50
            standby true
          </server>
        ]
        }
        before do
          driver.regular_nodes[0].stub(:available?).and_return(false)
          driver.regular_nodes[1].stub(:available?).and_return(false)
          driver.should_receive(:send_data).with(driver.standby_nodes[0], tag0, es)
          driver.should_receive(:send_data).with(driver.standby_nodes[0], tag1, es)
          driver.should_receive(:send_data).with(driver.standby_nodes[1], tag2, es)
        end
        it 'should forward to standby nodes considering weights' do
          driver.write_objects(tag0, es)
          driver.write_objects(tag1, es)
          driver.write_objects(tag2, es)
        end
      end
    end
  end
end

