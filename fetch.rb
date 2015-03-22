require 'weibo2'
require 'json'
require 'oauth2'
require 'rest_client'
require 'set'
require 'open-uri'
require 'date'

def mkdir(path)
  Dir.mkdir(path) if !Dir.exists?(path)
end

mkdir('data')
mkdir('data/public_timeline/all')
mkdir('data/public_timeline/all/json')
mkdir('data/public_timeline/target_weibos')
mkdir('data/public_timeline/target_weibos/images')
mkdir('data/public_timeline/target_weibos/json')

#$stdout.reopen("fetch.log", "a")
#$stdout.sync = true
#$stderr.reopen('fetch_error.log', 'a')

#Weibo2::Config.api_key = "2039168936"
#Weibo2::Config.api_secret = "3cb7d1e9f97010412ff878c4b9886319"
#Weibo2::Config.redirect_uri = "http://showgoers.eremzeit.com/smog"
#code1 = 'bc3aa76e3f6f7483ca617b3c93b00fe2'
client_id1 = "2039168936"
token1 = '2.009FBL4CYzIAOCe9b003a609XDzrXD'

#client1 = Weibo2::Client.from_code(code1)
#Weibo2::Config.api_key = "2830883582"
#Weibo2::Config.api_secret = "39f55897163c6b0051c6392f5a44c37e"
#Weibo2::Config.redirect_uri = "http://showgoers.eremzeit.com/smog"
#
#code2 = '563b8798eaf7bc59d39a15452a280133'
#client2 = Weibo2::Client.from_code(code2)
client_id2 = '2830883582'
token2 = '2.009FBL4CcIGaFDbeeabd5b3c4p8T6C'


#MIT_URBANUS : 2.00IEvqAC946trBdff4472408c2wt_E
#Processing test: 2.00IEvqACOIGl9B2637474aec0OJSBv
#CreativeCityTest:    2.00IEvqAC0_vTsycd1cc1df25d5hTRB
#CreativeCityTest2: 2.00IEvqACLRbk4Db4a066418cHVbl9B
#CreativeCityTest3: 2.00IEvqAChNfRpDe5ca3e168b0RVEW9
#CreativeCityTest4: 2.00IEvqACXX1jVD4a350edaf8JAxXfB
#CreativeCityTest5: 2.00IEvqAC4uAUZC3777d8e31101dmZw
#CreativeCityTest6: 2.00IEvqAC1baXzBe40171f9f0W_qG1C
#CreativeCityTest7: 2.00IEvqACddB2CD6f641ec0e5ukrV9C
#试验田1号： 2.00Oe8rnCaG2JvC0e38bea444BonZME
#试验田2号： 2.00Oe8rnC0ecbXf173a3b3652NTP6IC
#试验田3号:    2.00Oe8rnCYAliJE612e6edc8e0xe7Qg
#试验田4号:    2.00Oe8rnC0I_FiWe739a5364bTvJcaB
#试验田5号:    2.00Oe8rnCJpLoFDd5316a36f9IcImwC
#试验田6号:    2.00Oe8rnC23bgxC8d96ad7dbaULDhTB
#试验田7号:    2.00Oe8rnC1MhkwB6de53faa4dmqMMND
#试验田8号：2.00Oe8rnCG3bvrCca5471c55cLiZmSB
#试验田9号:   2.00Oe8rnC5_oFaCf9813055d4bxYhPD
#试验田10号：2.00Oe8rnCpd_DCD12bb182aab1YDQRD


#client2 = Weibo2::Client.new
#puts client2.auth_code.authorize_url(:response_type => "token")

class TimelineFetcher
  def initialize(creds, query_filters, proxies)
    @creds = creds
    @query_filters = query_filters
    @proxies = proxies
    @current_proxy = 0
    @ids = Set.new
  end

  def _image_url(weibo)
    #right now just get the largest
    labels = ['original_pic', 'bmiddle_pic', 'thumbnail_pic']
    for label in labels
      if weibo.has_key?(label)
        return weibo[label]
      end
    end
  end

  def _fetch_image(weibo, image_dir)
    return [] if !weibo.has_key?('pic_urls') || weibo['pic_urls'].count == 0

    begin
      pic_url = _image_url(weibo)

      file_ext = '.' + pic_url.split('.').last
      img_path = image_dir + weibo['id'].to_s + file_ext

      File.open(img_path, 'wb') do |fo|
        fo.write open(pic_url).read
      end

      img_path
    rescue e
      puts 'Error while fetching image!'
      puts e
      puts e.backtrace
    end
  end

  def update_proxy
    if @current_proxy == @proxies.length - 1
      @current_proxy = 0
    else
      @current_proxy += 1
    end

    RestClient.proxy = @proxies[@current_proxy]
    puts "Using proxy #{RestClient.proxy}"
  end

  def public_timeline_weibos(client_id, access_token, page)
    update_proxy

    r = RestClient.get('https://api.weibo.com/2/statuses/public_timeline.json', {:params => {:client_id => client_id, :access_token => access_token, :page => page}})
    if (r.code == 200)
      data = JSON.parse(r.body)
      statuses = data['statuses'].map do |weibo|
        weibo['fetched_at'] = Time.now.to_i
        weibo['fetched_on_page'] = page
        weibo
      end
      statuses
    else
      puts r.to_str
      nil
    end
  end

  def _fetch_image_for(weibos)
    weibos.each do |w|

      image_dir = './data/public_timeline/target_weibos/images/'
      if img_path = _fetch_image(w, image_dir)
        w['fetched_image_path'] = img_path
      end
    end

    weibos
  end

  def _filter_by_terms(weibos)
    weibos.select do |w|
      match = @query_filters.find {|query| w['text'].include?(query) }
      if match
        puts "Matching weibo #{w['id']} for term \"#{match}\""
      end

      !!match
    end
  end

  def fetch_loop
    weibos = []
    while true
      puts ""

      weibos = get_page_chunk

      if Set.new(weibos).count != weibos.length
        puts 'Duplicated ids!'
      end

      puts "Writing #{weibos.length} weibos to file"
      write_weibo_json(weibos, './data/public_timeline/all/json/')

      target_weibos = _filter_by_terms(weibos)
      puts "Fetched page of #{weibos.length} total items but filtered down to #{target_weibos.length}"
      write_weibo_json(target_weibos, './data/public_timeline/target_weibos/json/')
      _fetch_image_for(target_weibos)

      weibos.each do |w|
        id = w['id']
        if @ids.include?(id)
          puts "Already fetched id: #{id}"
        end

        @ids.add(id)
      end

      #sleep(40)
      sleep(20)
    end

  end

  def get_page_chunk
    current_page = 0
    max_page = 1

    weibos = []
    failed = false
    while current_page <= max_page || failed
      p = current_page % @creds.length
      client_id = @creds[p][0]
      token = @creds[p][1]

      puts "Fetching group of pages..."
      begin
        fetched_weibos = public_timeline_weibos(client_id, token, current_page)
        weibos += fetched_weibos
      rescue => e
        puts "Error while fetching timeline weibo: \n#{e}\n#{e.backtrace}"
        failed = true
      end

      sleep(1)
      current_page += 1
    end

    if weibos.length > 0
      range = _find_date_range_of_weibos(weibos)
      puts "Fetched weibos with range of #{range}"
    end

    weibos
  end

  def _find_date_range_of_weibos(weibos)
    earliest = nil
    latest = nil

    weibos.each do |w|
      created_at = w['created_at']
      next if created_at.nil?
      created_at = DateTime.parse(created_at).to_time.to_i
      if earliest.nil? || created_at < earliest
        earliest = created_at
      end

      if latest.nil? || created_at > latest
        latest = created_at
      end
    end

    [Time.at(earliest), Time.at(latest)]
  end

  def write_weibo_json(weibos, dir)
    return if !weibos || weibos.length == 0

    puts "Writing #{weibos.length} weibos to #{dir}"

    range = _find_date_range_of_weibos(weibos).map {|t| t.to_i}
    fname = "#{range[0]}__#{range[1]}.json" #convert to unix time

    File.open(dir + fname, 'w') do |fo|
      fo.write JSON.dump(weibos)
    end
  end
end

#污染 #pollution
#城市污染 #urban pollution
#污染投诉 #pollution report
#污染源 #the source of pollution
#note: rate limiting occurs also at the IP level.
#
#

creds = [[client_id1, token1], [client_id2, token2]]
proxies = ['http://ec2-52-10-95-176.us-west-2.compute.amazonaws.com:8888', 'http://ec2-52-11-186-242.us-west-2.compute.amazonaws.com:8888', 'http://ec2-52-11-181-167.us-west-2.compute.amazonaws.com:8888', 'http://ec2-52-11-216-78.us-west-2.compute.amazonaws.com:8888', 'http://ec2-52-11-222-51.us-west-2.compute.amazonaws.com:8888']
query_filters = ['污染', '城市污染', '污染投诉', '污染源', '大气污染', '随手拍黑烟', '随手拍污染', '为环保做点', '有种青年', '救救江豚']
fetcher = TimelineFetcher.new(creds, query_filters, proxies)
weibos = fetcher.fetch_loop
