# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"


# An example output that does nothing.
class LogStash::Outputs::Ses < LogStash::Outputs::Base
  
  config_name "ses"
  #Amazon web service account key 
  config :aws_key_id, :validate => :string
  #Amazon web service account secret key
  config :aws_sec_key, :validate => :string
  #Amazon web service targetted region where mail will be send
  config :region_name, :validate => :string

  #Verified AWS SES sender mail address , required
  config :from,:validate => :string, :required => true
  #Mail target users which seperated with comma
  config :to, :validate =>  :string, :default => ""
  #Mail cc users which seperated with comma
  config :cc, :validate =>  :string, :default => ""
  #Mail bcc users which seperated with comma
  config :bcc, :validate => :string, :default => ""
  #Mail subject
  config :subject, :validate => :string, :default => ""
  #Mail body which will be formatted with event values
  config :body, :validate => :string, :default => ""
  #Mail html body which will be formatted with event values
  config :htmlbody, :validate => :string, :default => ""
  #Mail reply address
  config :reply_to_addresses,:validate => :string, :default => ""

  public
  def register    
    require "aws-sdk-resources"
    options = {}
    if @aws_key_id && @aws_sec_key
      options[:access_key_id]     = @aws_key_id
      options[:secret_access_key] = @aws_sec_key
    end

    if @region_name
      options[:region] = @region_name
    end


    @ses = Aws::SES::Client.new(options)

    @to_addresses  = @to.split ","
    if @to_addresses.empty?
      logger.error("To can not nil.",:to => @to)      
      raise "SES 'To' area can not nil."
    end

    @cc_addresses  = @cc.split ","
    @bcc_addresses = @bcc.split ","

    @destination = {:to_addresses => @to_addresses }
      unless @cc_addresses.empty?
        @destination[:cc_addresses] = @cc_addresses
      end
      unless @bcc_addresses.empty?
        @destination[:bcc_addresses] = @bcc_addresses
      end

    @logger.debug("Aws SES Registered!", :config => options)
  end # def register

  public
  def receive(event)
    @logger.debug? and @logger.debug("Creating AWS SES mail with these settings : ", :message => event,:options => @options, :from => @from, :to => @to_addresses, :cc => @cc_addresses, :subject => @subject,:innerTarget => event.sprintf("%{mailTo}"))
    
    innerTarget = @destination.dup
    if event.include?('mailTo')
      innerTarget[:to_addresses] = event.sprintf("%{mailTo}").split ','      
      @logger.debug? and @logger.debug("new mailTo address found in event,overriding target addresses", :parsed => innerTarget[:to_addresses] )
    end
    
    subject = event.sprintf(@subject)
    body = event.sprintf(@body)
    htmlbody = event.sprintf(@htmlbody)
    message = { :subject => {:data => subject},
                 :body => {
                  :html => {:data => htmlbody },
                  :text => {:data => body }
                 }
               }

    begin 
      @logger.debug? and @logger.debug("Sending mail with these values : ", :from => @from, :to => innerTarget, :subject => @subject, :body => @body)
      @ses.send_email(
               :source => @from,
               :destination => innerTarget,               
               :message => message
      )
    rescue => e      
      logger.error("Something happen while delivering an SES email", :msg => e.message)
      @logger.debug? && @logger.debug("Processed event: ", :event => event)
    end    
  end # def event
end # class LogStash::Outputs::ses
