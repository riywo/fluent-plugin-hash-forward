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
  let(:driver) { Fluent::Test::OutputTestDriver.new(Fluent::HashForwardOutput, tag).configure(config) }

  describe 'test configure' do
    let(:tag) { 'test.tag' }
    let(:config) { CONFIG }
    context 'default behavior' do
      it { lambda{ driver }.should_not raise_error }
    end
  end

  describe 'test hashing' do
    let(:tag) { 'test.tag' }
    let(:config) { CONFIG }

    context 'test consistency' do
      before do
        @node = driver.instance.nodes(tag).first
      end
      it 'should forward to the same node' do
        expect(driver.instance.nodes(tag).first).to eq(@node)
      end
    end

    context 'test distribution' do
      let(:tag1) { 'test.tag1' }
      let(:tag2) { 'test.tag2' }
      before do
        MurmurHash3::V32.stub(:str_hash).with(tag1).and_return(0)
        MurmurHash3::V32.stub(:str_hash).with(tag2).and_return(1)
        @node1 = driver.instance.nodes(tag1).first
      end
      it 'should forward to the different node' do
        expect(driver.instance.nodes(tag2).first).not_to eq(@node1)
      end
    end

    context 'test hash_key' do
      let(:tag1) { 'test.tag1' }
      let(:tag2) { 'test.tag2' }
      let(:config) { CONFIG + %[hash_key ${tags[0..-2]}] }
      before do
        @node1 = driver.instance.nodes(tag1).first
      end
      it 'should forward to the different node' do
        expect(driver.instance.nodes(tag2).first).to eq(@node1)
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
        node = driver.instance.nodes(tag).first
        driver.instance.should_receive(:send_data).with(node, tag, es)
      end
      it 'should forward' do
        driver.instance.write_objects(tag, es)
      end
    end

    context 'test standby' do
      let(:config) {
        CONFIG + %[
        <server>
          host 192.168.1.5
          port 24224
          standby true
        </server>
        ]
      }
      before do
        Fluent::Engine.stub(:now).and_return(time)
        regular_node = driver.instance.nodes(tag)[0]
        standby_node = driver.instance.nodes(tag)[1]
        regular_node.stub(:available?).and_return(false) # stub as regular node is not available
        driver.instance.should_receive(:send_data).with(standby_node, tag, es)
      end
      it 'should forward to the standby node if regular node is not available' do
        driver.instance.write_objects(tag, es)
      end
    end
  end
end

