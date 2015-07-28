package org.catais.wfs.log

import groovy.util.logging.Log4j2;
import static groovy.io.FileType.*
import org.apache.commons.io.FilenameUtils
import java.util.zip.*
import java.util.StringTokenizer
import java.util.regex.*
import java.io.File
import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.model.*
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.time.LocalDateTime 
import java.time.Period



@Log4j2
class Main {

	static main(args) {
	
		def logDirectory = "/Users/stefan/Downloads/log/"
		def final NUM_FIELDS = 9
		def logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
		
		def url = Main.classLoader.getResource("GeoLite2-City.mmdb")
		def ipDatabase = new File(url.toURI())
		def dbReader = new DatabaseReader.Builder(ipDatabase).build();
		
//		def ipAddress = InetAddress.getByName("128.101.101.101");
//		
//		def ipResponse = dbReader.city(ipAddress);
//		
//		println ipResponse.getCity()
//		println ipResponse.getCountry()
//		println ipResponse.getCountry().getIsoCode()
		
		
		def ipMap = [:]
		def previousDate
		
		new File(logDirectory).eachFile(FILES) {file ->
			def fileName = file.getName()
			def fileExtension =  FilenameUtils.getExtension(fileName)
			
			log.debug fileName
			
			if (fileName.length() < 10) {
				log.debug "do nothing"
				return
			}
						
			if (fileName.substring(0, 10) != 'access.log') {
				log.debug "do nothing"
				return
			}
			
			def input
			if (fileExtension == 'gz') {
				input = new GZIPInputStream(new FileInputStream(file))
				
			} else {
				input  = new FileInputStream(file)
			}	
			
			def reader = new BufferedReader(new InputStreamReader(input))
			
			reader.eachLine{line ->				
				
				def p = Pattern.compile(logEntryPattern);
				def matcher = p.matcher(line)
									
				if (!matcher.matches() ) {
//					log.error "Bad log entry (or problem with RE?):"
//					log.error line
					return;
				  }
				
				def ip = matcher.group(1)
				def dateTime = matcher.group(4)
				def request = matcher.group(5)
				def response = matcher.group(6)
				def bytes = matcher.group(7)
				
				def formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:H:m:s Z", Locale.ENGLISH)
				def date = LocalDateTime.parse(dateTime, formatter)
				
				if (previousDate == null) {
					previousDate = date
				} else {
					if (date.isBefore(previousDate)) {
						previousDate = date
					}					
				}
				
				def referer = matcher.group(8)
				def browser = matcher.group(9)
				
				if (request.toLowerCase().indexOf('sogis/wfs/sogis.wfs') >= 0 && request.toLowerCase().indexOf('getfeature') >= 0) {
					log.debug request
					log.debug ip
					log.debug dateTime
					
					ipMap.put(ip, ip)
					
					try {
						def ipAddress = InetAddress.getByName(ip)
						def ipResponse = dbReader.city(ipAddress);
						log.debug "Country: " + ipResponse.getCountry()
					} catch (com.maxmind.geoip2.exception.AddressNotFoundException e) {
						log.error e.getMessage()
					}
				}
			}
		}
		log.info "First request: ${previousDate}"
		log.info ipMap.toString()
	}
}
