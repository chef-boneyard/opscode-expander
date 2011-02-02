#
# Author:: Daniel DeLeo (<dan@opscode.com>)
# Author:: Seth Falcon (<seth@opscode.com>)
# Author:: Chris Walters (<cw@opscode.com>)
# Copyright:: Copyright (c) 2010-2011 Opscode, Inc.
# License:: Apache License, Version 2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

require 'stringio'
require 'opscode/expander/configuration'

describe Expander::Configuration do
  before do
    @config = Expander::Configuration::Base.new
    @config.reset!
    @config.apply_defaults
    @config.index = 1
    @config.node_count = 5
  end

  it "stores the number of nodes" do
    @config.node_count.should == 5
  end

  it "stores the position in the node ring" do
    @config.index.should == 1
  end

  it "computes the vnodes the node should claim" do
    @config.vnode_numbers.should == (0..203).to_a
  end

  it "assigns any remainder to the last node in the ring" do
    @config.index = 5
    @config.vnode_numbers.should == (816..1023).to_a
  end

  it "raises an invalid config error when then node index is not set" do
    @config.index = nil
    lambda { @config.validate! }.should raise_error(Expander::Configuration::InvalidConfiguration)
  end

  it "raises an invalid config error when the node count is not set" do
    @config.node_count = nil
    lambda { @config.validate! }.should raise_error(Expander::Configuration::InvalidConfiguration)
  end

  it "raises an invalid config error when the index is greater than the node count" do
    @config.node_count = 5
    @config.index = 10
    lambda { @config.validate! }.should raise_error(Expander::Configuration::InvalidConfiguration)
  end

  it "exits when the config is invalid" do
    stdout = StringIO.new
    @config.reset!(stdout)
    @config.node_count = nil
    lambda {@config.fail_if_invalid}.should raise_error(SystemExit)
    stdout.string.should match(/You must specify this node's position in the ring as an integer/)
  end

  it "has a setting for solr url defaulting to localhost:8983" do
    @config.solr_url.should == "http://localhost:8983"
  end

  it "has a setting for the amqp host to connect to, defaulting to 0.0.0.0" do
    @config.amqp_host.should == '0.0.0.0'
  end

  it "has a setting for the amqp port to use, defaulting to 5672" do
    @config.amqp_port.should == '5672'
  end

  it "has a setting for the amqp_user, defaulting to 'chef'" do
    @config.amqp_user.should == 'chef'
  end

  it "has a setting for the amqp password, defaulting to 'testing'" do
    @config.amqp_pass.should == 'testing'
  end

  it "has a setting for the amqp vhost, defaulting to /chef" do
    @config.amqp_vhost.should == '/chef'
  end

  it "generates an AMQP configuration hash suitable for passing to Bunny.new or AMQP.start" do
    @config.amqp_config.should == {:host => '0.0.0.0', :port => '5672', :user => 'chef', :pass => 'testing', :vhost => '/chef'}
  end

  it "merges another config on top of itself" do
    other = Expander::Configuration::Base.new
    other.solr_url = "somewhere with non-pitiful disk io"
    @config.merge_config(other)
    @config.solr_url.should == "somewhere with non-pitiful disk io" #if only it was that easy
  end

  it "merges config settings so that defaults < config_file < command line " do
    config_file = File.dirname(__FILE__) + '/../fixtures/opscode-expander.rb'
    argv = ["-c", config_file, '-n', '23']
    Expander.config.reset!
    Expander.init_config(argv)
    Expander.config.amqp_pass.should == 'config-file'
    Expander.config.node_count.should == 23
  end

end