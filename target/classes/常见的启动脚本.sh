kafka的命令行
# 查看topic
kafka-topics.sh --bootstrap-server master:9093 --list
#创建topic
kafka-topics.sh --bootstrap-server master:9092 --create --topic a --partitions 3 --replication-factor 2
#发送消息
kafka-console-producer.sh --bootstrap-server master:9093  --topic a
kafka-console-producer.sh --bootstrap-server slave1:9094  --topic b
#消费消息 --from-beginning
kafka-console-consumer.sh --bootstrap-server slave1:9094  --topic a
#删除topic
kafka-topics.sh --bootstrap-server master:9092 --delete --topic a
#查看topic的详情
kafka-topics.sh --bootstrap-server master:9093 --describe --topic a
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
myzkServer.sh start
3开启kafka
mykafka.sh start
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

10.启动flink的jar包
#下面是前台启动，启动之后，可以关闭此界面
flink run -m yarn-cluster -c com.pdn.fenliu.SideOutput flinkdemo-1.0-SNAPSHOT.jar
#-n(--container)： TaskManager 的数量。此参数已经不推荐使用，以后不要再去使用这个参数，因为现在是根据需求，如果新的程序有需求，则直接向yarn申请新的TaskManager。
#如果写死呢，则以后资源不够，则会执行失败。
#比如TaskManager 的数量被设置为3，每个TaskManager的slot数量被设置为5.则并行度最大为15.如果此时在代码里面指定并行度为20，则会执行失败。
#-s(--slots)： 每个 TaskManager 的 slot 数量，默认一个 slot 一个 core，默认每个taskmanager 的 slot 的个数为 1， 有时可以多一些 taskmanager，做冗余。Slot数量建议和cup核数一致。
flink run -m yarn-cluster -n 3 -s 2  -c com.pdn.apitest.connectstreams.ControlConnectedStreams