require "test_helper"
require "fileutils"
require "securerandom"

class APITest < Test::Unit::TestCase
  def setup
    clean
    @uuid = '12345678-1234-5678-1234-567812345678'
    @cons = Traildb::TrailDBConstructor.new('testtrail', ['field1', 'field2'])
    @cons.add(@uuid, 1, ['a', '1'])
    @cons.add(@uuid, 2, ['b', '2'])
    @cons.add(@uuid, 3, ['c', '3'])
    @cons.finalize
  end

  def teardown
    clean
  end

  def clean
    FileUtils.rm_f 'testtrail.tdb'
    FileUtils.rm_rf 'testtrail'
    FileUtils.rm_f 'whitelist_testtrail.tdb'
    FileUtils.rm_rf 'whitelist_testtrail'
    FileUtils.rm_f 'blacklist_testtrail.tdb'
    FileUtils.rm_rf 'blacklist_testtrail'
  end

  def test_uuid_func
    uuid = SecureRandom.uuid
    uuid_raw = Traildb.uuid_raw(uuid)
    assert_equal uuid, Traildb.uuid_hex(uuid_raw)
  end

  def test_trails
    db = Traildb::TrailDB.new('testtrail')
    assert_equal 1, db.num_trails

    trail = db.trail(0)
    assert_instance_of Traildb::TrailDBCursor, trail

    events = trail.to_a
    assert_equal 3, events.size
    events.each do |event|
      assert_respond_to event, 'time'
      assert_respond_to event, 'field1'
      assert_respond_to event, 'field2'
    end
  end

  def test_trails_selected_uuids
    uuids = ["02345678-1234-5678-1234-567812345678",
             "12345678-1234-5678-1234-567812345678",
             "22345678-1234-5678-1234-567812345678",
             "32345678-1234-5678-1234-567812345678",
             "42345678-1234-5678-1234-567812345678"]
    cons = Traildb::TrailDBConstructor.new('whitelist_testtrail', ['field1', 'field2'])
    uuids.each do |uuid|
      cons.add(uuid, 1, ['a', '1'])
      cons.add(uuid, 2, ['b', '2'])
      cons.add(uuid, 3, ['c', '3'])
    end
    cons.finalize()

    tdb = Traildb::TrailDB.new('whitelist_testtrail')
    whitelist = [uuids[0],
                 uuids[3],
                 uuids[4]]

    n = 0
    tdb.trails(selected_uuids: whitelist).each do |trail_uuid, trail_events|
      n += 1
      trail_events = trail_events.to_a
      assert_includes whitelist, trail_uuid
      assert_equal 3, trail_events.size
    end
    assert_equal 3, n
  end

  def test_crumbs
    db = Traildb::TrailDB.new('testtrail')
    n = 0
    db.trails.each do |uuid, trail|
      n += 1
      assert_equal @uuid, uuid
      assert_instance_of Traildb::TrailDBCursor, trail
      assert_equal 3, trail.to_a.size
    end
    assert_equal 1, n
  end

  def test_silly_open
    assert_equal true, File.exist?('testtrail.tdb')
    assert_equal false, File.exist?('testtrail')

    Traildb::TrailDB.new('testtrail.tdb')
    Traildb::TrailDB.new('testtrail')

    assert_raise Traildb::TrailDBError do
      Traildb::TrailDB.new('foo.tdb')
    end
  end

  def test_fields
    db = Traildb::TrailDB.new('testtrail')
    assert_equal ['time', 'field1', 'field2'], db.fields
  end

  def test_uuids
    db = Traildb::TrailDB.new('testtrail')
    assert_equal 0, db.get_trail_id(@uuid)
    assert_equal @uuid, db.get_uuid(0)
    assert_includes db, @uuid
  end

  def test_lexicons
    db = Traildb::TrailDB.new('testtrail')
    # First field
    assert_equal 4, db.lexicon_size(1)
    assert_equal ['a', 'b', 'c'], db.lexicon(1).to_a
    # Second field
    assert_equal ['1', '2', '3'], db.lexicon(2).to_a
    assert_raise Traildb::TrailDBError do
      db.lexicon(3) # Out of bounds
    end
  end

  def test_metadata
    db = Traildb::TrailDB.new('testtrail')
    assert_equal 1, db.min_timestamp()
    assert_equal 3, db.max_timestamp()
    assert_equal [1, 3], db.time_range()
    assert_equal [1, 3], db.time_range(parsetime: false)
  end

  def test_apply_whitelist
    uuids = ["02345678-1234-5678-1234-567812345678",
             "12345678-1234-5678-1234-567812345678",
             "22345678-1234-5678-1234-567812345678",
             "32345678-1234-5678-1234-567812345678",
             "42345678-1234-5678-1234-567812345678"]
    cons = Traildb::TrailDBConstructor.new('whitelist_testtrail', ['field1', 'field2'])
    uuids.each do |uuid|
      cons.add(uuid, 1, ['a', '1'])
      cons.add(uuid, 2, ['b', '2'])
      cons.add(uuid, 3, ['c', '3'])
    end
    cons.finalize()

    tdb = Traildb::TrailDB.new('whitelist_testtrail')
    whitelist = [uuids[0],
                 uuids[3],
                 uuids[4]]
    tdb.apply_whitelist(whitelist)
    found_trails = tdb.trails(parsetime: false).to_a

    assert_equal uuids.size, found_trails.size
    found_trails.each do |trail_uuid, trail_events|
      expected_length = whitelist.include?(trail_uuid) ? 3 : 0
      assert_equal expected_length, trail_events.to_a.size
    end
  end

  def test_apply_blacklist
    uuids = ["02345678-1234-5678-1234-567812345678",
             "12345678-1234-5678-1234-567812345678",
             "22345678-1234-5678-1234-567812345678",
             "32345678-1234-5678-1234-567812345678",
             "42345678-1234-5678-1234-567812345678"]
    cons = Traildb::TrailDBConstructor.new('blacklist_testtrail', ['field1', 'field2'])
    uuids.each do |uuid|
      cons.add(uuid, 1, ['a', '1'])
      cons.add(uuid, 2, ['b', '2'])
      cons.add(uuid, 3, ['c', '3'])
    end
    cons.finalize()

    tdb = Traildb::TrailDB.new('blacklist_testtrail')
    blacklist = [uuids[1],
                 uuids[2]]
    tdb.apply_blacklist(blacklist)
    found_trails = tdb.trails(parsetime: false).to_a

    assert_equal uuids.size, found_trails.size
    found_trails.each do |trail_uuid, trail_events|
      expected_length = blacklist.include?(trail_uuid) ? 0 : 3
      assert_equal expected_length, trail_events.to_a.size
    end
  end
end

class FilterTest < Test::Unit::TestCase
  def setup
    clean
    @uuid = '12345678-1234-5678-1234-567812345678'
    @cons = Traildb::TrailDBConstructor.new('testtrail', ['field1', 'field2', 'field3'])
    @cons.add(@uuid, 1, ['a', '1', 'x'])
    @cons.add(@uuid, 2, ['b', '2', 'x'])
    @cons.add(@uuid, 3, ['c', '3', 'y'])
    @cons.add(@uuid, 4, ['d', '4', 'x'])
    @cons.add(@uuid, 5, ['e', '5', 'x'])
    @cons.finalize
  end

  def teardown
    clean
  end

  def clean
    FileUtils.rm_f 'testtrail.tdb'
    FileUtils.rm_rf 'testtrail'
  end

  def test_simple_disjunction
    tdb = Traildb::TrailDB.new('testtrail')
    # test shorthand notation (not a list of lists)
    events = tdb.trail(0, event_filter: [['field1', 'a'], ['field2', '4']]).to_a
    assert_equal 2, events.size
    assert_equal ['a', '1'], [events[0].field1, events[0].field2]
    assert_equal ['d', '4'], [events[1].field1, events[1].field2]
  end

  def test_negation
    tdb = Traildb::TrailDB.new('testtrail')
    events = tdb.trail(0, event_filter: [['field3', 'x', true]]).to_a
    assert_equal 1, events.size
    assert_equal ['c', '3', 'y'], [events[0].field1, events[0].field2, events[0].field3]
  end

  def test_conjunction
    tdb = Traildb::TrailDB.new('testtrail')
    events = tdb.trail(0, event_filter: [[['field1', 'e'], ['field1', 'c']],
                                               [['field3', 'y', true]]]).to_a
    assert_equal 1, events.size
    assert_equal ['e', '5'], [events[0].field1, events[0].field2]
  end

  def test_time_range
    tdb = Traildb::TrailDB.new('testtrail')
    events = tdb.trail(0, event_filter: [[[2, 4]]], parsetime: false).to_a
    assert_equal 2, events.size
    assert_equal 2, events[0].time
    assert_equal 3, events[1].time
  end

  def test_filter_object
    tdb = Traildb::TrailDB.new('testtrail')
    obj = tdb.create_filter([[['field1', 'e'], ['field1', 'c']],
                             [['field3', 'y', true]]])
    events = tdb.trail(0, event_filter: obj).to_a
    assert_equal 1, events.size
    assert_equal ['e', '5'], [events[0].field1, events[0].field2]
    events = tdb.trail(0, event_filter: obj).to_a
    assert_equal 1, events.size
    assert_equal ['e', '5'], [events[0].field1, events[0].field2]
  end
end

class ConsTest < Test::Unit::TestCase
  def teardown
    clean
  end

  def clean
    FileUtils.rm_f 'testtrail.tdb'
    FileUtils.rm_rf 'testtrail'
    FileUtils.rm_f 'testtrail2.tdb'
    FileUtils.rm_rf 'testtrail2'
  end

  def test_cursor
    uuid = '12345678-1234-5678-1234-567812345678'
    cons = Traildb::TrailDBConstructor.new('testtrail', ['field1', 'field2'])
    cons.add(uuid, 1, ['a', '1'])
    cons.add(uuid, 2, ['b', '2'])
    cons.add(uuid, 3, ['c', '3'])
    cons.add(uuid, 4, ['d', '4'])
    cons.add(uuid, 5, ['e', '5'])
    tdb = cons.finalize

    assert_raise IndexError do
      tdb.get_trail_id('12345678-1234-5678-1234-567812345679')
    end

    trail = tdb.trail(tdb.get_trail_id(uuid))
    assert_equal nil, trail.to_enum.size

    j = 1
    trail.each do |event|
      assert_equal j, event.field2.to_i
      assert_equal j, event.time.to_i
      j += 1
    end
    assert_equal 6, j

    # Iterator is empty now
    assert_equal([], trail.to_a)

    field1_values = tdb.trail(tdb.get_trail_id(uuid)).map(&:field1)
    assert_equal ['a', 'b', 'c', 'd', 'e'], field1_values
  end

  def test_cursor_parsetime
    uuid = '12345678-1234-5678-1234-567812345678'
    cons = Traildb::TrailDBConstructor.new('testtrail', ['field1'])

    events = [[Time.new(2016, 1, 1, 1, 1), ['1']],
              [Time.new(2016, 1, 1, 1, 2), ['2']],
              [Time.new(2016, 1, 1, 1, 3), ['3']]]
    events.each do |time, fields|
      cons.add(uuid, time, fields)
    end
    tdb = cons.finalize

    timestamps = tdb.trail(0, parsetime: true).map(&:time)

    assert_instance_of Time, timestamps[0]
    assert_equal timestamps, events.map{|time, _|time}
    assert_equal tdb.time_range(parsetime: true), [events[0][0], events[-1][0]]
  end

  def test_binarydata
    binary = '\x00\x01\x02\x00\xff\x00\xff'
    uuid = '12345678-1234-5678-1234-567812345678'
    cons = Traildb::TrailDBConstructor.new('testtrail', ['field1'])
    cons.add(uuid, 123, [binary])
    tdb = cons.finalize
    assert_equal binary, tdb[0].to_a[0].field1
  end

  def test_cons
    uuid = '12345678-1234-5678-1234-567812345678'
    cons = Traildb::TrailDBConstructor.new('testtrail', ['field1', 'field2'])
    cons.add(uuid, 123, ['a'])
    cons.add(uuid, 124, ['b', 'c'])
    tdb = cons.finalize

    assert_equal 0, tdb.get_trail_id(uuid)
    assert_equal uuid, tdb.get_uuid(0)
    assert_equal 1, tdb.num_trails
    assert_equal 2, tdb.num_events
    assert_equal 3, tdb.num_fields

    crumbs = tdb.trails.to_a
    assert_equal 1, crumbs.size
    assert_equal uuid, crumbs[0][0]
    assert tdb[uuid]
    assert_includes tdb, uuid
    assert_not_includes tdb, '00000000000000000000000000000000'
    assert_raise IndexError do
      tdb['00000000000000000000000000000000']
    end
    trail = crumbs[0][1].to_a

    assert_equal 123, trail[0].time
    assert_equal 'a', trail[0].field1
    assert_equal '', trail[0].field2 # TODO: Should this be None?

    assert_equal 124, trail[1].time
    assert_equal 'b', trail[1].field1
    assert_equal 'c', trail[1].field2
  end

  def test_items
    uuid = '12345678-1234-5678-1234-567812345678'
    cons = Traildb::TrailDBConstructor.new('testtrail', ['field1', 'field2'])
    cons.add(uuid, 123, ['a', 'x' * 2048])
    cons.add(uuid, 124, ['b', 'y' * 2048])
    tdb = cons.finalize

    cursor = tdb.trail(0, rawitems: true).to_enum
    event = cursor.next
    assert_equal 'a', tdb.get_item_value(event.field1)
    assert_equal 'x' * 2048, tdb.get_item_value(event.field2)
    assert_equal tdb.get_item('field1', 'a'), event.field1
    assert_equal tdb.get_item('field2', 'x' * 2048), event.field2
    event = cursor.next
    assert_equal 'b', tdb.get_item_value(event.field1)
    assert_equal 'y' * 2048, tdb.get_item_value(event.field2)
    assert_equal tdb.get_item('field1', 'b'), event.field1
    assert_equal tdb.get_item('field2', 'y' * 2048), event.field2

    cursor = tdb.trail(0, rawitems: true).to_enum
    event = cursor.next
    field = Traildb.tdb_item_field(event.field1)
    val = Traildb.tdb_item_val(event.field1)
    assert_equal 'a', tdb.get_value(field, val)
    field = Traildb.tdb_item_field(event.field2)
    val = Traildb.tdb_item_val(event.field2)
    assert_equal 'x' * 2048, tdb.get_value(field, val)
    event = cursor.next
    field = Traildb.tdb_item_field(event.field1)
    val = Traildb.tdb_item_val(event.field1)
    assert_equal 'b', tdb.get_value(field, val)
    field = Traildb.tdb_item_field(event.field2)
    val = Traildb.tdb_item_val(event.field2)
    assert_equal 'y' * 2048, tdb.get_value(field, val)
  end

  def test_append
    uuid = '12345678-1234-5678-1234-567812345678'
    cons = Traildb::TrailDBConstructor.new('testtrail', ['field1'])
    cons.add(uuid, 123, ['foobarbaz'])
    tdb = cons.finalize

    cons = Traildb::TrailDBConstructor.new('testtrail2', ['field1'])
    cons.add(uuid, 124, ['barquuxmoo'])
    cons.append(tdb)
    tdb = cons.finalize

    assert_equal 2, tdb.num_events
    uuid, trail = tdb.trails.to_a[0]
    trail = trail.to_a
    assert_equal [123, 124], trail.map(&:time)
    assert_equal ['foobarbaz', 'barquuxmoo'], trail.map(&:field1)
  end
end
