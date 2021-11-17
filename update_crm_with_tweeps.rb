#!/usr/bin/env ruby

require 'bundler/inline'
require 'set'

gemfile do
    source 'https://rubygems.org'
    gem 'twitter', '7.0.0'
    gem 'airrecord', '~> 1.0.7'
    gem 'byebug', platforms: [:mri, :mingw, :x64_mingw]
end

class TwitterWrapper
    def initialize(options={})
        @client = Twitter::REST::Client.new do |config|
            config.consumer_key = ENV["TWITTER_API_KEY"]
            config.consumer_secret = ENV["TWITTER_API_KEY_SECRET"]
            config.bearer_token = ENV["TWITTER_BEARER_TOKEN"]
            config.access_token = ENV["TWITTER_ACCESS_TOKEN"]
            config.access_token_secret = ENV["TWITTER_ACCESS_TOKEN_SECRET"]
        end
    end

    def client
        @client
    end
end

class Person < Airrecord::Table
    self.table_name = "People"
    self.base_key = ENV["AIRTABLE_BASE_KEY"]

    def self.tweep_ids
        @@tweep_ids ||= Person.__tweep_ids
    end

    def self.person_attributes(tweep={})
        tweep_attributes = Hash.new
        return tweep_attributes if (tweep.nil? || tweep.empty?)

        tweep_attributes["Name"] = tweep[:name]
        tweep_attributes["Twitter ID"] = tweep[:id_str]
        tweep_attributes["Twitter Name"] = tweep[:name]
        tweep_attributes["Twitter Screen Name"] = tweep[:screen_name]
        tweep_attributes["Twitter Location"] = tweep[:location]
        tweep_attributes["Twitter Verified"] = tweep[:verified]
        tweep_attributes["Twitter Followers Count"] = tweep[:followers_count]
        tweep_attributes["Twitter Following Count"] = tweep[:friends_count]

        tweep_attributes
    end

    private

    def self.__tweep_ids
        people = Person.all
        people.map { |person| person["Twitter ID"].to_i }.to_set
    end
end

class ExcludedTweep < Airrecord::Table
    self.table_name = "Excluded Tweeps"
    self.base_key = ENV["AIRTABLE_BASE_KEY"]

    def self.tweep_ids
        @@tweep_ids ||= ExcludedTweep.__tweep_ids
    end

    private
    
    def self.__tweep_ids
        excluded_tweeps = ExcludedTweep.all
        excluded_tweeps.map { |tweep| tweep["Twitter ID (from Person)"].first.to_i }.to_set
    end
end

def update_crm_with_tweeps(options={})
    twitter = TwitterWrapper.new
    client = twitter.client
    tweeps = []
    next_cursor = -1
    tweep_count = 0
    cursor_count = 0
    loop do
        begin
            tweeps_cursor = client.friends(
                screen_name: ENV["TWITTER_HANDLE"], 
                count: 200, 
                cursor: next_cursor
            )
        rescue Twitter::Error::TooManyRequests => error
            puts "Twitter API rate limit exceeded. Sleeping for 15 minutes."
            sleep error.rate_limit.reset_in + 1
            retry
        end
        tweeps_cursor.attrs[:users].each do |tweep|
            tweeps << tweep
            tweep_count += 1
            puts "Tweep Count: #{tweep_count}"
        end
        cursor_count += 1
        puts "Cursor Count: #{cursor_count}"
        next_cursor = tweeps_cursor.attrs[:next_cursor]
        break if (next_cursor.nil? || next_cursor.zero?)
    end
    return if tweeps.empty?

    Airrecord.api_key = ENV["AIRTABLE_API_KEY"]

    tweeps.each do |tweep|
        next if ExcludedTweep.tweep_ids.include?(tweep[:id])
        if Person.tweep_ids.include?(tweep[:id])
            person = Person.all(
                filter: "{Twitter ID} = \"#{tweep[:id]}\"",
                max_records: 1
            ).first
            Person.person_attributes(tweep).each do |key, value|
                person[key] = value
            end
            person.save
        else
            Person.create(Person.person_attributes(tweep))
        end
    end
end

update_crm_with_tweeps if (__FILE__ == $0)
