kafka的命令行
# 查看topic
kafka-topics.sh --bootstrap-server master:9093 --list
#创建topic
kafka-topics.sh --bootstrap-server master:9092 --create --topic a --partitions 3 --replication-factor 2
#发送消息
kafka-console-producer.sh --bootstrap-server master:9093  --topic a
kafka-console-producer.sh --bootstrap-server slave1:9094  --topic a
#消费消息 --from-beginning
kafka-console-consumer.sh --bootstrap-server slave1:9094  --topic a
#删除topic
kafka-topics.sh --bootstrap-server master:9092 --delete --topic a
#查看topic的详情
kafka-topics.sh --bootstrap-server master:9092 --describe --topic a
#查看所有的topic
kafka-topics.sh --bootstrap-server master:9092 --list
# 启动
kafka-server-start.sh -daemon config/server.properties
# 启动kafka镜像生成容器
docker run -d --privileged=true -itd -v /sys/fs/cgroup:/sys/fs/cgroup  --net netgroup --ip 172.18.0.6 -p 9094:9094 --name  kafkaMasterTwo  -e KAFKA_BROKER_ID=1 -e KAFKA_ZOOKEEPER_CONNECT=172.18.0.4:2181/kafkaMasterTwo -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://172.20.10.5:9094 -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9094 -v /etc/localtime:/etc/localtime -u root -h kafkaMasterTwo  wurstmeister/kafka




#自己的脚本位于的位置/usr/local/bin
#启动集群的脚本
1重新设置/etc/hosts
mynode_init.sh
2开启zk
myzkServer.sh
3开启kafka
mykafka.sh
4开启hdfs注意是在master上执行下面的命令
start-dfs.sh
5在slaver1上执行下面的命令
start-yarn.sh

7.访问的ui界面地址
yran
http://localhost:8088/cluster/nodes
hdfs
http://localhost:50070/dfshealth.html#tab-overview

8.拷贝本地的包到容器里面
docker cp /Users/zhihu/Desktop/project/flink_test/target/flink_test-1.0-SNAPSHOT.jar master:/opt/softwares

9.启动redis
cd /opt/modules/redis7/bin
#启动
./redis-server redis.conf
#关闭
./redis-cli shutdown
#链接客户端
./redis-cli -h 127.0.0.1 -p 6379