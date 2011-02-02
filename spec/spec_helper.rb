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

require 'pp'
require 'stringio'
require 'rubygems'
require 'bunny'
$:.unshift(File.dirname(__FILE__) + '/../lib/')
require 'chef/expander'

include Chef

OPSCODE_EXPANDER_MQ_CONFIG = {:user => "guest", :pass => "guest", :vhost => '/opscode-expander-test'}

begin
  b = Bunny.new(OPSCODE_EXPANDER_MQ_CONFIG)
  b.start
  b.stop
rescue Bunny::ProtocolError, Bunny::ServerDownError, Bunny::ConnectionError => e
  STDERR.puts(<<-ERROR)

****************************** FAIL *******************************************
* Running these tests requires a running instance of rabbitmq
* You also must configure a vhost "/opscode-expander-test"
* and a user "guest" with password "guest" with full rights
* to that vhost
-------------------------------------------------------------------------------
> rabbitmq-server
> rabbitmqctl add_vhost /opscode-expander-test
> rabbitmqctl set_permissions -p /opscode-expander-test guest '.*' '.*' '.*'
> rabbitmqctl list_user_permissions guest
****************************** FAIL *******************************************

ERROR
  if ENV['DEBUG'] == "true"
    STDERR.puts("#{e.class.name}: #{e.message}")
    STDERR.puts("#{e.backtrace.join("\n")}")
  end
  exit(1)
end

