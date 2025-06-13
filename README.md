自动同步mongodb数据库到clickhouse的超级轻量级中间件，占用系统资源少，已内置消息队列，不需要安装额外软件，开箱即用。生成环境实践中，每天同步过亿数据量。

```
开始使用：
./mgo2ck -f start -c ./mgo2ck.config.template.json
启动成功输出：
[2025-06-13 14:33:25.5975] [demo] [YKRC5DOaSBjUiklNKMQA] INFO main.main.func1:main.go:22 start success... 

重载配置：
kill -10 进程号 

优雅推出：
kill -15 进程号
```
