module Fluent

class PgJsonOutput < Fluent::BufferedOutput
  Fluent::Plugin.register_output('pgjson', self)

  config_param :host,           :string,      default: 'localhost'
  config_param :port,           :integer,     default: 5432
  config_param :sslmode,        :string,      default: 'prefer'
  config_param :database,       :string
  config_param :table,          :string
  config_param :user,           :string,      default: nil
  config_param :password,       :string,      default: nil, secret: true
  config_param :time_col,       :string,      default: 'time'
  config_param :tag_col,        :string,      default: 'tag'
  config_param :record_col,     :string,      default: 'record'
  config_param :record_ext_map, :hash,        default: nil
  config_param :msgpack,        :bool,        default: false

  def initialize
    super
    require 'pg'
    @conn = nil
  end

  def configure(conf)
    super
    if @record_ext_map && !@record_ext_map.empty?
      ext_cols = @record_ext_map.values.join(', ')
      @has_ext = true
      @copy_cmd =
        "COPY #{@table} (#{@tag_col}, #{@time_col}, #{ext_cols}, #{@record_col}) FROM STDIN WITH DELIMITER E'\\x01' NULL '%NULL%'"
    else
      @has_ext = false
      @copy_cmd =
        "COPY #{@table} (#{@tag_col}, #{@time_col}, #{@record_col}) FROM STDIN WITH DELIMITER E'\\x01'"
    end
  end

  def shutdown
    super

    if ! @conn.nil? and ! @conn.finished?
      @conn.close()
    end
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def write(chunk)
    init_connection
    @conn.exec @copy_cmd
    begin
      chunk.msgpack_each do |tag, time, record|
        if @has_ext
          ext_cols = @record_ext_map.values
          ext_record = record.each_with_object({}) do |(k,v), out|
            unless ext_cols.include?(k)
              out[k] = v
            end
          end
          ext_values = ext_cols.map do |k|
            record[k].nil? ? '%NULL%' : record[k]
          end
          ext = ext_values.join("\x01") + "\x01"
        else
          ext = ""
        end

        @conn.put_copy_data "#{tag}\x01#{Time.at(time).to_s}\x01#{ext}#{record_value(ext_record)}\n"
      end
    rescue => err
      errmsg = "%s while copy data: %s" % [ err.class.name, err.message ]
      @conn.put_copy_end( errmsg )
      @conn.get_result
      raise
    else
      @conn.put_copy_end
      res = @conn.get_result
      raise res.result_error_message if res.result_status!=PG::PGRES_COMMAND_OK
    end
  end

  private
  def init_connection
    if @conn.nil?
      $log.debug "connecting to PostgreSQL server #{@host}:#{@port}, database #{@database}..."

      begin
        @conn = PGconn.new(:dbname => @database, :host => @host, :port => @port, :sslmode => @sslmode, :user => @user, :password => @password)
      rescue
        if ! @conn.nil?
          @conn.close()
          @conn = nil
        end
        raise "failed to initialize connection: #$!"
      end
    end
  end

  def record_value(record)
    if @msgpack
      "\\#{@conn.escape_bytea(record.to_msgpack)}"
    else
      json = record.to_json
      json.gsub!(/\\/){ '\\\\' }
      json
    end
  end
end

end
