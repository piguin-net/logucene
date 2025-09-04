/**
 * 特定のログをDiscordへ通知するサンプル
 * ログの内容は以下から取得可能です
 *   doc.timestamp
 *   doc.date
 *   doc.time
 *   doc.host
 *   doc.addr
 *   doc.port
 *   doc.facility
 *   doc.severity
 *   doc.format
 *   doc.message
 *   doc.raw
 */

@Grapes(
    @Grab(group='org.slf4j', module='slf4j-api', version='2.0.17')
)

import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

class DiscordNotifier {

    def logger = LoggerFactory.getLogger(this.class)
    def URL = System.getProperty('discord.url', null)
    def PATTERN = /Login failed/

    def check(doc) {
        if (URL != null && doc.message =~ PATTERN) {
            logger.info('pattern matched.')
            notify(URL, doc)
        }
    }

    def notify(url, doc) {
        def http = HttpClient.newHttpClient()
        def request = HttpRequest.newBuilder(URI.create(url))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("""{"username": "logucene", "content": "${doc.message}"}"""))
            .build()
        def response = http.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() != 204) {
            throw new Exception(response.body())
        }
    }

}

new DiscordNotifier().check(doc)
