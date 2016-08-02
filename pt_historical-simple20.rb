# Stripped down version of pt_historica.rb

require "json"          #PowerTrack speaks json.
require "yaml"          #Used for configuration, job and rules files.
require "optparse"
require "base64"

#Other PowerTrack classes.
require_relative "./pt_rules"
require_relative "./pt_job_description"
require_relative "./pt_restful"

#=======================================================================================================================
#Object for marshalling a Historical job through the process.
#One object per job.  Creates one HTTP and one Job object...

class PtHistorical

    attr_accessor :http,:job, :uuid, :base_url, :url, :job_url, :account_name, :user_name, :password_encoded,
                  :stream_type, :quote, :results, :accept

    def initialize(config_file, job_description_file, accept=nil)
        #class variables.
        # http://gnip-api.gnip.com/historical/powertrack/accounts/{ACCOUNT_NAME}/publishers/twitter/jobs.json
        @@base_url = "http://gnip-api.gnip.com/historical/powertrack/accounts/"

        if accept.nil? #Client did not explicitly set 'accept' so set to false (reject).
            @accept = nil
        else
            if accept == "true" then
                @accept = true
            else
                @accept = false
            end
        end

        getSystemConfig(config_file)  #Load the oHistorical PowerTrack account details.

        @url = constructURL  #Spin up Historical URL.
        puts "@url Variable is: #{@url}"

        #Set up a HTTP object.
        @http = PtRESTful.new(@url, @user_name, @password_encoded)  #Historical API is REST based (currently)
        
        #Set up a Job object.
        @job = JobDescription.new  #Create a Job object.
        @job.getConfig(job_description_file) #Load the configuration.
    end


    def getSystemConfig(config_file)

        config = YAML.load_file(config_file)

        #Config details.
        @account_name = config["account"]["account_name"]
        @user_name  = config["account"]["user_name"]
        @password_encoded = config["account"]["password_encoded"]

        if @password_encoded.nil? then  #User is passing in plain-text password...
            @password = config["account"]["password"]
            @password_encoded = Base64.encode64(@password)
        end

        @stream_type = config["historical"]["stream_type"]

    end

    #Returns UUID assigned to this job.
    def getUUID(jobs, title = nil)
        # if no title passed in, set to object instance value.
        if title.nil? then
            title = @job.title
        end

        #Load response into JSON and extract the UUID
        jobs = JSON.parse(jobs)
        jobs = jobs["jobs"]
        #p jobs.length
        jobs.each { |job|
            #p job
            if job["title"] == title then
                #Split the URL by "/", grab the last, then split by "." and grab the first.
                @job_url = job["jobURL"]
                @uuid  = @job_url.split("/").last.split(".").first
            end
        }
        @uuid
    end

    def constructURL(account_name=nil)
                                 #{ACCOUNT_NAME}/publishers/twitter/jobs.json
        if not account_name.nil? then
            return @@base_url + account_name + "/publishers/twitter/jobs.json"
        else
            return @@base_url + @account_name + "/publishers/twitter/jobs.json"
        end

        #If you use the previous one... it works: 'https://historical.gnip.com/accounts/CUResearch/jobs.json'
    end

    def setResultsDetails(job)
        @results = {}
        job = JSON.parse(job)
        @results = job["results"]
    end


    '''
    Submit the Job to the Historical PowerTrack server.
    Check the server response.
    Return whether it was successfully submitted.
    '''
    def submitJob
        jobAdded = false

        #Submit Job for estimation.
        data = @job.getJobDescription
        
        response = @http.POST(data)

        p response

        #Read response and update status if successful...  Notify on problem.
        if response.code.to_i >= 200 and response.code.to_i < 300 then
            #Update status if OK
            jobAdded = true
            p "Job submitted, sleeping for one minute..."
            sleep (60*1)
        else
            p "HTTP error code: " + response.code + " | " + response.body  #Print HTTP error.
            jobAdded = false
        end

        jobAdded
    end

    #Simple (silly?) wrappers to remove JSON formatting from Job object user.
    def acceptJob
        "{\"status\":\"accept\"}"

    end

    def rejectJob
        "{\"status\":\"reject\"}"
    end


    #This method marshalls a job through the process...
    def manageJob

        jobList = getJobList  #We start by retrieving a list of our jobs.
        status = getStatus(jobList)  #Based on payload, determine status.  New job?  If not, where in process?
        p "Status of " + @job.title + " job: " + status.to_s

        if status.name == "error" then
            p "ERROR occurred with your Historical Job request. Quitting"
            exit(status = false)
        end


        #If this is a NEW job, assemble the Job description and submit it.
        #This includes managing this Job's rules.
        if status.name == "new" then
            if !submitJob then
                p "ERROR occurred with your Historical Job request. Quitting"
                exit(status = false)
            end
        end

        #Confirm job was submitted OK, ask the PowerTrack server again...
        #Whether this is a new job, or one already submitted, determine the Job UUID for current job.
        if status.name == "new" then
            @accept = nil #New jobs should always have accept=nil (regardless of what was passed in)
            response = @http.GET #Get the job list again, confirm it was submitted OK, and lead us to the uuid.
            jobList = response.body
            #TODO: confirm the current job is in list.
            status.name = "estimating"
        end

        @uuid = getUUID(jobList)  #This call sets both oJob.uuid and oJob.jobURL

        #From this point on, we are operating on the JOB URL.
        @http.url = @job_url #Update the HTTP object's URL to this Job's URL.
        setOutputFolder(@output_folder)  #Now that we have the uuid, set the output directory for the Historical daa.
        response = http.GET
        jobInfo = response.body

        status = getStatus(jobInfo)

        if status.name == "estimating" then  #loop until the estimate is finished and we've moved to "quoted"
            @accept = nil #New jobs should always have accept=nil (regardless of what was passed in)
            #Check to see if estimation is finished.  If not pause 5 minutes and recheck
            until status.name == "quoted"
                p "Estimate not ready yet, sleeping for 5 minutes..."
                sleep(5*60)
                response = @http.GET
                jobInfo = response.body
                status = getStatus(jobInfo)
            end
        end

        '''
        At this point the job has been quoted and is ready for approval or rejection.  Therefore, there should be some
        mechanism to review the job quote details.  In a system with a UI, the job quote would be presented for review.

        There is an "accept" parameter passed into this script.  If accept is set to "true", and the job has been
        quoted, then it will charge ahead and run the job.  If "accept" is set to "true" and the quote has not been
        generated, the accept=true will be ignored.

        IMPORTANT NOTE: during Historical PowerTrack trials, the accept/reject process is not enabled through the API.
        Instead Gnip will need to perform this manually. After an account becomes live with a subscription, then jobs
        can be accepted/rejected using the API.
        '''

        if status.name == "quoted" then
            #Display the quote
            setQuoteDetails(jobInfo)
            p "Job (" + @job.title + ") has been quoted. | " + @quote.to_s

            if @accept then
                #Accept Job.
                response = @http.PUT(acceptJob) #Accept job.
                if response.code.to_i >= 200 and response.code.to_i < 300 then
                    status.name = "accepted"
                    p "Job (" + @job.title + ") was ACCEPTED."
                else
                    p "Error occurred.  Job (" + @job.title + ") could not be accepted. "
                end
            elsif @accept == nil
                p "Job (" + @job.title + ") has been quoted, and needs to be ACCEPTED or REJECTED. "
                p "Script can be reran with '-a true' or -a false'"
            elsif not @accept
                response = @http.PUT(rejectJob) #Reject job.
                if response.code.to_i >= 200 and response.code.to_i < 300 then
                    status.name = "rejected"
                    p "Job (" + @job.title + ") was REJECTED."
                else
                    p "ERROR occurred.  Job (" + @job.title + ") could not be rejected (but still at 'quoted' stage). "
                end

            end
        end

        #If accepted, monitor status of Job completion.
        if status.name == "accepted" or status.name == "running" then
            #Check to see if job is finished.  If not pause 5 minutes and recheck
            until status.name == "finished"
                p "Job is running... " + status.percent.to_s + "% finished."
                sleep(5*60)
                response = @http.GET
                jobInfo = response.body
                status = getStatus(jobInfo)
            end
        end

        #If completed, retrieve the files.
        #Server provides a JSON file with paths to the flatfiles...
        #These files can be downloaded in parallel to a local directory...
        #http://support.gnip.com/customer/portal/articles/745678-retrieving-data-for-a-delivered-job
        if status.name == "finished" then

            downloadData(jobInfo)
            handleSuspectMinutes(jobInfo)
            if @storage == "files" then
                uncompressData
            end

        end
    end


end #PtHistorical class.





#=======================================================================================================================
#-----------------------------------------
#Usage examples and unit testing:
#-----------------------------------------

if __FILE__ == $0  #This script code is executed when running this file.

    OptionParser.new do |o|
        o.on('-c CONFIG') { |config| $config = config}
        o.on('-j JOB') { |job| $job = job}
        o.on('-a ACCEPT') { |accept| $accept = accept}  #Pass in accept = true/false, otherwise stop at quote and display it.
        o.parse!
    end

    #TODO: Handle this better....
    if $config.nil? then
        $config = "./PowerTrackConfig_private.yaml"  #Default
    end

    if $job.nil? then
        $job = "./jobDescriptions/HistoricalRequest.yaml" #Default
    end

    #Create a Historical PowerTrack object, passing in an account configuration file and a job description file.
    oHistPT = PtHistorical.new($config, $job, $accept)

    p oHistPT.http.GET_ACCCOUNT_USAGE.body

    # p oHistPT.http.GET.body

    #The "do all" method, utilizes many other methods to complete a job.
    # p oHistPT.manageJob

    p "Exiting"


end
