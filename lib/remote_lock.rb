class RemoteLock
  class Error < RuntimeError; end

  DEFAULT_OPTIONS = {
    :initial_wait => 10e-3, # seconds -- first soft fail will wait for 10ms
    :expiry       => 60,    # seconds
    :retries      => 11,    # these defaults will retry for a total 41sec max
    :linear_wait  => false, # when true, retry wait time is initial_wait +/- a small randomizer (instead of the default logrithmic wait algorithm)
  }

  def initialize(adapter, prefix = nil)
    raise "Invalid Adapter" unless Adapters::Base.valid?(adapter)
    @adapter = adapter
    @prefix = prefix
  end

  def synchronize(key, options={})
    if acquired?(key)
      yield
    else
      acquire_lock(key, options)
      begin
        yield
      ensure
        release_lock(key)
      end
    end
  end

  # Read lock process
  # Get master lock
  # Increment readers counter
  # Release master lock
  # Do work
  # Decrement readers counter
  def synchronize_reader(key, options={})
    acquire_lock(key, options)
    add_reader(key, options)
    release_lock(key)
    begin
      yield
    ensure
      remove_reader(key)
    end
  end

  # Write lock process
  # Get master lock
  # If readers > 0, wait for their completion
  # Do work
  # Release master lock
  def synchronize_writer(key, options={})
    if acquired?(key)
      yield
    else
      acquire_lock(key, options)
      begin
        wait_for_readers(key, options)
        yield
      ensure
        release_lock(key)
      end
    end
  end

  def acquire_lock(key, options = {})
    options = DEFAULT_OPTIONS.merge(options)
    1.upto(options[:retries]) do |attempt|
      success = @adapter.store(key_for(key), options[:expiry])
      return if success
      break if attempt == options[:retries]
      Kernel.sleep(2 ** (attempt + rand - 1) * options[:initial_wait]) unless options[:linear_wait]
      Kernel.sleep( (options[:initial_wait] * rand) - (options[:initial_wait] /2.0) + options[:initial_wait] ) if options[:linear_wait]
    end
    raise RemoteLock::Error, "Couldn't acquire lock for: #{key}"
  end

  def release_lock(key)
    @adapter.delete(key_for(key))
  end

  def acquired?(key)
    @adapter.has_key?(key_for(key))
  end

  def add_reader(key, options = {})
    @adapter.incr(counter_for(key), options[:expiry])
  end

  def remove_reader(key)
    @adapter.decr(counter_for(key))
  end

  def wait_for_readers(key, options = {})
    options = DEFAULT_OPTIONS.merge(options)
    1.upto(options[:retries]) do |attempt|
      return unless @adapter.is_positive?(counter_for(key))
      break if attempt == options[:retries]
      Kernel.sleep(2 ** (attempt + rand - 1) * options[:initial_wait]) unless options[:linear_wait]
      Kernel.sleep( (options[:initial_wait] * rand) - (options[:initial_wait] /2.0) + options[:initial_wait] ) if options[:linear_wait]
    end
    raise RemoteLock::Error, "Timeout waiting for readers on: #{key}"
  end

  private

  def key_for(string)
    return [@prefix, "lock", string].compact.join('|')
  end

  def counter_for(string)
    [@prefix, "lock", string, "counter"].compact.join('|')
  end

end

require 'remote_lock/adapters/memcached'
require 'remote_lock/adapters/redis'
