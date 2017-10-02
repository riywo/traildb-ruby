require 'traildb'

SESSION_LIMIT = 30 * 60

def sessions(tdb)
  tdb.trails(only_timestamp: true).each_with_index do |(uuid, trail), i|
    trail = trail.to_enum
    prev_time = trail.next
    num_events = 1
    num_sessions = 1
    trail.each do |timestamp|
      if timestamp - prev_time > SESSION_LIMIT
        num_sessions += 1
      end
      prev_time = timestamp
      num_events += 1
    end
    puts 'Trail[%d] Number of Sessions: %d Number of Events: %d' % [i, num_sessions, num_events]
  end
end

if ARGV.size < 1
  puts 'Usage: tutorial_wikipedia_sessions <wikipedia-history.tdb>'
else
  sessions(Traildb::TrailDB.new(ARGV[0]))
end
