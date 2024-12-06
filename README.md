配置都在pom.xml里面
目前实现了一个爬虫 抓取flink官网上的信息然后发送到kafka里

安装kafka
brew install kafka
启动zookeeper
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

打开新的终端
启动kafka
kafka-server-start /usr/local/etc/kafka/server.properties

打开新的终端
接受爬虫的数据
kafka-console-consumer --topic flink-web-scraped-data --from-beginning --bootstrap-server localhost:9092
 