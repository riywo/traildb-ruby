require "traildb/version"

require "ffi"

module Traildb
  extend FFI::Library
  ffi_lib "traildb"

  typedef :pointer, :tdb
  typedef :pointer, :tdb_cons
  typedef :uint32, :tdb_field
  typedef :uint64, :tdb_val
  typedef :uint64, :tdb_item
  typedef :pointer, :tdb_cursor
  typedef :int, :tdb_error
  typedef :pointer, :tdb_event_filter

  class TdbEvent < FFI::Struct
    layout(
      :timestamp, :uint64,
      :num_items, :uint64,
      :items, [:tdb_item, 0],
    )

    def tdb_items(valuefun = nil)
      address = pointer + offset_of(:items)
      items = FFI::Pointer.new(:uint64, address).read_array_of_uint64(self[:num_items])
      valuefun.nil? ? items : items.map{|i|valuefun.call(i)}
    end
  end

  class TdbOptValue < FFI::Union
    layout(
      :ptr, :pointer,
      :value, :uint64,
    )
  end

  TDB_OPT_EVENT_FILTER = 101

  attach_function :tdb_cons_init, [], :tdb_cons
  attach_function :tdb_cons_open, [:tdb_cons, :string, :pointer, :uint64], :tdb_error
  attach_function :tdb_cons_close, [:tdb_cons], :void
  attach_function :tdb_cons_add, [:tdb_cons, :pointer, :uint64, :pointer, :pointer], :tdb_error
  attach_function :tdb_cons_append, [:tdb_cons, :tdb], :tdb_error
  attach_function :tdb_cons_finalize, [:tdb_cons], :tdb_error

  attach_function :tdb_init, [], :tdb
  attach_function :tdb_open, [:tdb, :string], :tdb_error
  attach_function :tdb_close, [:tdb], :void

  attach_function :tdb_lexicon_size, [:tdb, :tdb_field], :tdb_error

  attach_function :tdb_get_field, [:tdb, :string], :tdb_error
  attach_function :tdb_get_field_name, [:tdb, :tdb_field], :string

  attach_function :tdb_get_item, [:tdb, :tdb_field, :pointer, :uint64], :tdb_item
  attach_function :tdb_get_value, [:tdb, :tdb_field, :tdb_val, :pointer], :string
  attach_function :tdb_get_item_value, [:tdb, :tdb_item, :pointer], :string

  attach_function :tdb_get_uuid, [:tdb, :uint64], :pointer
  attach_function :tdb_get_trail_id, [:tdb, :pointer, :pointer], :tdb_error

  attach_function :tdb_error_str, [:tdb_error], :string

  attach_function :tdb_num_trails, [:tdb], :uint64
  attach_function :tdb_num_events, [:tdb], :uint64
  attach_function :tdb_num_fields, [:tdb], :uint64
  attach_function :tdb_min_timestamp, [:tdb], :uint64
  attach_function :tdb_max_timestamp, [:tdb], :uint64

  attach_function :tdb_version, [:tdb], :uint64

  attach_function :tdb_cursor_new, [:tdb], :tdb_cursor
  attach_function :tdb_cursor_free, [:tdb_cursor], :void
  attach_function :tdb_cursor_next, [:tdb_cursor], TdbEvent.ptr
  attach_function :tdb_get_trail, [:tdb_cursor, :uint64], :tdb_error
  attach_function :tdb_get_trail_length, [:tdb_cursor], :uint64
  attach_function :tdb_cursor_set_event_filter, [:tdb_cursor, :tdb_event_filter], :tdb_error

  attach_function :tdb_event_filter_new, [], :tdb_event_filter
  attach_function :tdb_event_filter_add_term, [:tdb_event_filter, :tdb_item, :int], :tdb_error
  attach_function :tdb_event_filter_add_time_range, [:tdb_event_filter, :uint64, :uint64], :tdb_error
  attach_function :tdb_event_filter_new_clause, [:tdb_event_filter], :tdb_error
  attach_function :tdb_event_filter_new_match_none, [], :tdb_event_filter
  attach_function :tdb_event_filter_new_match_all, [], :tdb_event_filter
  attach_function :tdb_event_filter_free, [:tdb_event_filter], :void

  attach_function :tdb_set_opt, [:tdb, :uint, TdbOptValue.by_value], :tdb_error
  attach_function :tdb_set_trail_opt, [:tdb, :uint64, :uint, TdbOptValue.by_value], :tdb_error

  def self.uuid_hex(uuid)
    if uuid.is_a? FFI::Pointer
      ary = uuid.read_bytes(16).unpack("NnnnnN")
      uuid = "%08x-%04x-%04x-%04x-%04x%08x" % ary
    end
    uuid
  end

  def self.uuid_raw(uuid)
    if uuid.is_a? String
      ptr = FFI::MemoryPointer.new(:uint8, 16)
      uuid = ptr.write_bytes(uuid.scan(/[0-9a-f]{2}/).map{|x|x.to_i(16)}.pack('C*'))
    end
    uuid
  end

  def self.tdb_item_is32(item); (item & 128) == 0 end
  def self.tdb_item_field32(item); item & 127 end
  def self.tdb_item_val32(item); (item >> 8) & 4294967295 end # UINT32_MAX

  def self.tdb_item_field(item)
    if tdb_item_is32(item)
      tdb_item_field32(item)
    else
      (item & 127) | (((item >> 8) & 127) << 7)
    end
  end

  def self.tdb_item_val(item)
    if tdb_item_is32(item)
      tdb_item_val32(item)
    else
      item >> 16
    end
  end

  class TrailDBError < ::StandardError
    def initialize(message, error = 0)
      message += ": " + Traildb.tdb_error_str(error) if error != 0
      super message
    end
  end

  class TrailDBConstructor < FFI::AutoPointer
    def initialize(path, ofields=[])
      raise TrailDBError.new("Path is required") if path.nil?
      super Traildb.tdb_cons_init()
      n = ofields.size
      ofield_names = FFI::MemoryPointer.new(:string, n)
      ofield_names.write_array_of_pointer(ofields.map{|field|FFI::MemoryPointer.from_string(field)})
      ret = Traildb.tdb_cons_open(self, path, ofield_names, n)
      raise TrailDBError.new("Cannot open constructor", ret) if ret != 0
      @path = path
      @ofields = ofields
    end

    def add(uuid, tstamp, values)
      tstamp = tstamp.to_i if tstamp.is_a? Time
      uuid = Traildb.uuid_raw(uuid)
      n = @ofields.size
      value_array = FFI::MemoryPointer.new(:string, n)
      value_array.write_array_of_pointer(values.map{|v|v.nil? ? nil : FFI::MemoryPointer.from_string(v)})
      value_lengths = FFI::MemoryPointer.new(:uint64, n)
      value_lengths.write_array_of_uint64(values.map{|v|v.nil? ? 0 : v.size})
      ret = Traildb.tdb_cons_add(self, uuid, tstamp, value_array, value_lengths)
      raise TrailDBError.new("Too many values: %s" % values[ret]) if ret != 0
    end

    def append(db)
      f = Traildb.tdb_cons_append(self, db)
      if f < 0
        raise TrailDBError.new("Wrong number of fields: %d" % db.num_fields, f)
      elsif f > 0
        raise TrailDBError.new("Too many values", f)
      end
    end

    def finalize
      ret = Traildb.tdb_cons_finalize(self)
      raise TrailDBError.new("Could not finalize", ret) if ret != 0
      TrailDB.new(@path)
    end

    def self.release(ptr)
      Traildb.tdb_cons_close(ptr)
    end
  end

  class TrailDBCursor < FFI::AutoPointer
    include Enumerable

    def initialize(cursor, cls, valuefun, parsetime, only_timestamp, event_filter_obj)
      super cursor
      @cls = cls
      @valuefun = valuefun
      @parsetime = parsetime
      @only_timestamp = only_timestamp
      if event_filter_obj
        @event_filter_obj = event_filter_obj
        ret = Traildb.tdb_cursor_set_event_filter(self, event_filter_obj)
        raise TrailDBError.new("cursor_set_event_filter failed", ret) if ret != 0
      end
    end

    def each
      loop do
        event = Traildb.tdb_cursor_next(self)
        break if event.null?
        timestamp = event[:timestamp]
        timestamp = Time.at(timestamp) if @parsetime
        if @only_timestamp
          yield timestamp
        else
          items = event.tdb_items(@valuefun)
          yield @cls.new(timestamp, *items)
        end
      end
    end

    def self.release(ptr)
      Trailsdb.tdb_cursor_free(ptr)
    end
  end

  class TrailDB < FFI::AutoPointer
    attr_reader :num_trails, :num_events, :num_fields, :fields

    def initialize(path)
      super Traildb.tdb_init()
      ret = Traildb.tdb_open(self, path)
      raise TrailDBError.new("Could not open %s" % path, ret) if ret != 0
      @num_trails = Traildb.tdb_num_trails(self)
      @num_events = Traildb.tdb_num_events(self)
      @num_fields = Traildb.tdb_num_fields(self)
      @fields = @num_fields.times.map{|i|Traildb.tdb_get_field_name(self,i)}
      @event_cls = Struct.new(*@fields.map(&:to_sym))
      @uint64_ptr = FFI::MemoryPointer.new(:uint64)
    end

    def include?(uuidish)
      self[uuidish]
      true
    rescue IndexError
      false
    end

    def [](uuidish)
      if uuidish.is_a? String
        trail(get_trail_id(uuidish))
      else
        trail(uuidish)
      end
    end

    def size
      @num_trails
    end

    def trails(selected_uuids: nil, **kwds)
      if selected_uuids.nil?
        size.times.each.lazy.map do |i|
          [get_uuid(i), trail(i, kwds)]
        end
      else
        selected_uuids.each.lazy.map do |uuid|
          begin
            i = get_trail_id(uuid)
          rescue IndexError
            next
          end
          [uuid, trail(i, kwds)]
        end
      end
    end

    def trail(trail_id, parsetime: false, rawitems: false, only_timestamp: false, event_filter: nil)
      cursor = Traildb.tdb_cursor_new(self)
      ret = Traildb.tdb_get_trail(cursor, trail_id)
      raise TrailDBError.new("Failed to create cursor", ret) if ret != 0
      valuefun = rawitems ? nil : ->(item){get_item_value(item)}
      event_filter_obj = case event_filter
        when TrailDBEventFilter
          event_filter
        when Array
          create_filter(event_filter)
        else
          nil
      end
      TrailDBCursor.new(cursor, @event_cls, valuefun, parsetime, only_timestamp, event_filter_obj)
    end

    def field(fieldish)
      if fieldish.is_a? String
        fieldish = @fields.index(fieldish)
      end
      fieldish
    end

    def lexicon(fieldish)
      field = field(fieldish)
      (1..lexicon_size(field)-1).lazy.map{|i|
        get_value(field, i)
      }
    end

    def lexicon_size(fieldish)
      field = field(fieldish)
      value = Traildb.tdb_lexicon_size(self, field)
      raise TrailDBError.new("Invalid field index") if value == 0
      value
    end

    def get_item(fieldish, value)
      field = field(fieldish)
      item = Traildb.tdb_get_item(self, field, value, value.size)
      raise TrailDBError.new("No such value: '%s'" % value) if item.nil?
      item
    end

    def get_item_value(item)
      value = Traildb.tdb_get_item_value(self, item, @uint64_ptr)
      raise TrailDBError.new("Error reading value") if value.nil?
      value.slice(0, @uint64_ptr.read_uint64)
    end

    def get_value(fieldish, val)
      field = field(fieldish)
      value = Traildb.tdb_get_value(self, field, val, @uint64_ptr)
      raise TrailDBError.new("Error reading value") if value.nil?
      value.slice(0, @uint64_ptr.read_uint64)
    end

    def get_uuid(trail_id, raw=false)
      uuid = Traildb.tdb_get_uuid(self, trail_id)
      if uuid.nil?
        raise ::IndexError
      else
        raw ? uuid.read_string : Traildb.uuid_hex(uuid)
      end
    end

    def get_trail_id(uuid)
      ret = Traildb.tdb_get_trail_id(self, Traildb.uuid_raw(uuid), @uint64_ptr)
      raise ::IndexError if ret != 0
      @uint64_ptr.read_uint64
    end

    def time_range(parsetime: false)
      tmin = min_timestamp
      tmax = max_timestamp
      parsetime ? [Time.at(tmin), Time.at(tmax)] : [tmin, tmax]
    end

    def min_timestamp
      Traildb.tdb_min_timestamp(self)
    end

    def max_timestamp
      Traildb.tdb_max_timestamp(self)
    end

    def create_filter(event_filter)
      TrailDBEventFilter.new(self, event_filter)
    end

    def apply_whitelist(uuids)
      empty_filter = Traildb.tdb_event_filter_new_match_none()
      all_filter = Traildb.tdb_event_filter_new_match_all()
      value = TdbOptValue.new
      value[:ptr] = empty_filter
      Traildb.tdb_set_opt(self, TDB_OPT_EVENT_FILTER, value)
      value[:ptr] = all_filter
      uuids.each do |uuid|
        begin
          trail_id = get_trail_id(uuid)
          Traildb.tdb_set_trail_opt(self, trail_id, TDB_OPT_EVENT_FILTER, value)
        rescue IndexError
          next
        end
      end
    end

    def apply_blacklist(uuids)
      empty_filter = Traildb.tdb_event_filter_new_match_none()
      all_filter = Traildb.tdb_event_filter_new_match_all()
      value = TdbOptValue.new
      value[:ptr] = all_filter
      Traildb.tdb_set_opt(self, TDB_OPT_EVENT_FILTER, value)
      value[:ptr] = empty_filter
      uuids.each do |uuid|
        begin
          trail_id = get_trail_id(uuid)
          Traildb.tdb_set_trail_opt(self, trail_id, TDB_OPT_EVENT_FILTER, value)
        rescue IndexError
          next
        end
      end
    end

    def self.release(ptr)
      Traildb.tdb_close(ptr)
    end
  end

  class TrailDBEventFilter < FFI::AutoPointer
    def initialize(db, query)
      super Traildb.tdb_event_filter_new()
      if !query[0][0].is_a? Array
        query = [query]
      end
      query.each_with_index do |clause, i|
        if i > 0
          ret = Traildb.tdb_event_filter_new_clause(self)
          raise TrailDBError.new("Out of memory in create_filter", ret) if ret != 0
        end
        clause.each do |term|
          ret = 0
          if term.size == 2 and term[0].is_a? Integer and term[0].is_a? Integer
            start_time, end_time = term
            ret = Traildb.tdb_event_filter_add_time_range(self, start_time, end_time)
          else
            is_negative = false
            if term.size == 3
              field, value, is_negative = term
            else
              field, value = term
            end
            item = begin
              db.get_item(field, value)
            rescue TrailDBError, ValueError
              0
            end
            ret = Traildb.tdb_event_filter_add_term(self, item, is_negative ? 1 : 0)
          end
          raise TrailDBError.new("Out of memory in create_filter", ret) if ret != 0
        end
      end
    end

    def self.release(ptr)
      Traildb.tdb_event_filter_free(ptr)
    end
  end
end
