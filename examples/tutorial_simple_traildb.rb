require 'traildb'
require 'securerandom'

cons = Traildb::TrailDBConstructor.new('tiny', ['username', 'action'])

3.times.each do |i|
  uuid = SecureRandom.uuid
  username = 'user%d' % i
  ['open', 'save', 'close'].each_with_index do |action, day|
    cons.add(uuid, Time.new(2016, i + 1, day + 1), [username, action])
  end
end

cons.finalize

Traildb::TrailDB.new('tiny').trails.each do |uuid, trail|
  puts uuid, trail.to_a
end
