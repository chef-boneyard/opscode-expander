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

require 'yajl'
require 'fast_xs'
require 'em-http-request'
require 'opscode/expander/loggable'
require 'opscode/expander/flattener'
require 'redis'

module Opscode
  module Expander
    class Redisizer

      include Loggable

      ADD     = "add"
      DELETE  = "delete"
      SKIP    = "skip"

      ITEM        = "item"
      ID          = "id"
      TYPE        = "type"
      DATABASE    = "database"
      ENQUEUED_AT = "enqueued_at"

      X_CHEF_id_CHEF_X        = 'X_CHEF_id_CHEF_X'
      X_CHEF_database_CHEF_X  = 'X_CHEF_database_CHEF_X'
      X_CHEF_type_CHEF_X      = 'X_CHEF_type_CHEF_X'

      CONTENT_TYPE_XML = {"Content-Type" => "text/xml"}

      attr_reader :action

      attr_reader :indexer_payload

      attr_reader :chef_object

      attr_reader :obj_id

      attr_reader :obj_type

      attr_reader :database

      def initialize(object_command_json, &on_completion_block)
        @on_completion_block = on_completion_block
        if parsed_message    = parse(object_command_json)
          @action           = parsed_message["action"]
          @indexer_payload  = parsed_message["payload"]

          extract_object_fields if @indexer_payload
        else
          @action = SKIP
        end
      end

      def extract_object_fields
        @chef_object = @indexer_payload[ITEM]
        @database    = @indexer_payload[DATABASE]
        @obj_id      = @indexer_payload[ID]
        @obj_type    = @indexer_payload[TYPE]
        @enqueued_at = @indexer_payload[ENQUEUED_AT]
      end

      def parse(serialized_object)
        Yajl::Parser.parse(serialized_object)
      rescue Yajl::ParseError
        log.error { "cannot index object because it is invalid JSON: #{serialized_object}" }
      end

      def run
        case @action
        when ADD
          add
        when DELETE
          delete
        when SKIP
          completed
          log.info { "not indexing this item because of malformed JSON"}
        else
          completed
          log.error { "cannot index object becuase it has an invalid action #{@action}" }
        end
      end

      def add
        post_to_solr(pointyize_add) { "indexed #{indexed_object} transit-time[#{transit_time}s]" }
      rescue Exception => e
        log.error { "#{e.class.name}: #{e.message}\n#{e.backtrace.join("\n")}"}
      end

      def delete
        post_to_solr(pointyize_delete) { "deleted #{indexed_object} transit-time[#{transit_time}s]"}
      rescue Exception => e
        log.error { "#{e.class.name}: #{e.message}\n#{e.backtrace.join("\n")}"}
      end

      def flattened_object
        flattened_object = Flattener.new(@chef_object).flattened_item
 
        flattened_object[X_CHEF_id_CHEF_X]        = [@obj_id]
        flattened_object[X_CHEF_database_CHEF_X]  = [@database]
        flattened_object[X_CHEF_type_CHEF_X]      = [@obj_type]

        log.debug {"adding flattened object to Solr: #{flattened_object.inspect}"}

        flattened_object
      end

      def send_to_redis
        redis = redis_init
        sep = '\001'
        flattened_object.each do |field, values|
          values.each do |value|
            key = [@database, @obj_type, field, value].join(sep)
            redis.sadd(key, @obj_id)
        end
      end

      # Takes a succinct document id, like 2342, and turns it into something
      # even more compact, like
      #   "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<delete><id>2342</id></delete>\n"
      def pointyize_delete
        xml = ""
        xml << START_XML
        xml << DELETE_DOC
        xml << ID_OPEN
        xml << @obj_id.to_s
        xml << ID_CLOSE
        xml << END_DELETE
        xml
      end

      def post_to_solr(document, &logger_block)
        log.debug("POSTing document to SOLR:\n#{document}")
        http_req = EventMachine::HttpRequest.new(solr_url).post(:body => document, :timeout => 1200, :head => CONTENT_TYPE_XML)
        http_req.callback do
          completed
          if http_req.response_header.status == 200
            log.info(&logger_block)
          else
            log.error { "Failed to post to solr: #{indexed_object}" }
          end
        end
        http_req.errback do
          completed
          log.error { "Failed to post to solr (connection error): #{indexed_object}" }
        end
      end

      def completed
        @on_completion_block.call
      end

      def transit_time
        Time.now.utc.to_i - @enqueued_at
      end

      def redis_init
        # TODO: configure host/port here
        Redis.new
      end

      def indexed_object
        "#{@obj_type}[#{@obj_id}] database[#{@database}]"
      end

    end
  end
end
