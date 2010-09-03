require 'yajl'
require 'fast_xs'
require 'em-http-request'
require 'opscode/expander/loggable'
require 'opscode/expander/flattener'

module Opscode
  module Expander
    class Solrizer

      include Loggable

      ADD     = "add"
      DELETE  = "delete"
      SKIP    = "skip"

      ITEM     = "item"
      ID       = "id"
      TYPE     = "type"
      DATABASE = "database"

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

      def initialize(object_command_json)
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
          log.info { "not indexing this item because of malformed JSON"}
        else
          log.error { "cannot index object becuase it has an invalid action #{@action}" }
        end
      end

      def add
        post_to_solr(pointyize_add)
      rescue Exception => e
        log.error { "#{e.class.name}: #{e.message}\n#{e.backtrace.join("\n")}"}
      end

      def delete
        post_to_solr(pointyize_delete)
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

      START_XML   = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      ADD_DOC     = "<add><doc>"
      DELETE_DOC  = "<delete>"
      ID_OPEN     = "<id>"
      ID_CLOSE    = "</id>"
      END_ADD_DOC = "</doc></add>\n"
      END_DELETE  = "</delete>\n"
      FIELD_ATTR  = '<field name="'
      FIELD_ATTR_END = '">'
      CLOSE_FIELD = "</field>"

      # Takes a flattened hash where the values are arrays and converts it into
      # a dignified XML document suitable for POST to Solr.
      def pointyize_add
        xml = ""
        xml << START_XML << ADD_DOC

        flattened_object.each do |field, values|
          values.each do |v|
            xml << FIELD_ATTR
            xml << field
            xml << FIELD_ATTR_END
            xml << v.fast_xs
            xml << CLOSE_FIELD
          end
        end
        xml << END_ADD_DOC
        xml
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

      def post_to_solr(document)
        log.debug("POSTing document to SOLR:\n#{document}")
        http_req = EventMachine::HttpRequest.new(solr_url).post(:body => document, :timeout => 180, :head => CONTENT_TYPE_XML)
        http_req.callback do
          if http_req.response_header.status == 200
            log.info { "successfully indexed #{indexed_object} t(#{Time.now.to_i})" }
          else
            log.error { "Failed to post to solr: #{indexed_object}" }
          end
        end
      end

      def solr_url
        'http://127.0.0.1:8983/solr/update'
      end

      def indexed_object
        "#{@obj_type}[#{@obj_id}] in database #{@database}"
      end

    end
  end
end