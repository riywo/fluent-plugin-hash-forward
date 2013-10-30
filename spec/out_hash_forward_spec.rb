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

    # ToDo: FIX TO MAKE THIS WORK
    #<server>
    #  host 192.168.1.5
    #  port 24224
    #  standby true
    #</server>

    # ToDo: FIX TO MAKE THIS WORK
    #<secondary>
    #  type file
    #  path /var/log/fluent/forward-failed
    #</secondary>
  ]
  let(:driver) { Fluent::Test::OutputTestDriver.new(Fluent::HashForwardOutput, tag).configure(config) }

  describe 'test configure' do
    let(:tag) { 'test.tag' }
    let(:config) { CONFIG }
    context 'default behavior' do
      it { lambda{ driver }.should_not raise_error }
    end
  end

  describe 'test emit' do
    let(:tag) { 'test.tag' }
    let(:time) { Time.now.to_i }
    let(:es) { Fluent::OneEventStream.new(time, {"a"=>1}) }
    let(:chain) { Fluent::NullOutputChain.instance }
    let(:config) { CONFIG }

    context 'default behavior' do
      before do
        Fluent::Engine.stub(:now).and_return(time)
        Fluent::ForwardOutput.any_instance.should_receive(:emit).with(tag, es, chain)
      end
      it 'should forward' do
        driver.instance.emit(tag, es, chain)
      end
    end
    
    context 'test remove_prefix' do
      let(:tag) { 'test.tag' }
      let(:config) { CONFIG + %[remove_prefix test] }
      before do
        Fluent::Engine.stub(:now).and_return(time)
        Fluent::ForwardOutput.any_instance.should_receive(:emit).with('tag', es, chain)
      end
      it 'should forward with removing prefix' do
        driver.instance.emit(tag, es, chain)
      end
    end

    context 'test add_prefix' do
      let(:tag) { 'test.tag' }
      let(:config) { CONFIG + %[add_prefix add] }
      before do
        Fluent::Engine.stub(:now).and_return(time)
        Fluent::ForwardOutput.any_instance.should_receive(:emit).with("add.#{tag}", es, chain)
      end
      it 'should forward with adding prefix' do
        driver.instance.emit(tag, es, chain)
      end
    end
  end
end

