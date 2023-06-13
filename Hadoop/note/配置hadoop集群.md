# 配置虚拟机网卡

```bash
vim /etc/sysconfig/network-scripts/ifcfg-ens33
```
```text
IPADDR="192.168.88.101"
NETMASK="255.255.255.0"
GATEWAY="192.168.88.2"
DNS1="192.168.88.2"
```
# 重启网络服务
```bash
systemctl restart network
```

# 配置 hosts
```bash
192.168.88.101 node1
192.168.88.102 node2
192.168.88.103 node3
```

# 生成密钥
```bash
ssh-keygen -t rsa -b 4096
```

# 设置免密登录
```bash
ssh-copy-id node1
ssh-copy-id node2
ssh-copy-id node3
```

# 添加 hadoop 用户
```bash
useradd hadoop
passwd hadoop
su - hadoop
```

# 生成密钥
```bash
ssh-keygen -t rsa -b 4096
```

# 解压 jdk
```bash
tar -zxvf jdk-8u361-linux-x64.tar.gz -C /export/server/
ln -s /export/server/jdk1.8.0_361 jdk
```

# 配置 jdk 环境变量
```bash
vim /etc/profile
export JAVA_HOME=/export/server/jdk
export PATH=$PATH:$JAVA_HOME/bin
source /etc/profile
```

# 移除之前的软连接

```bash
rm -f /usr/bin/java
```

# 创建软连接
```bash
ln -s /export/server/jdk/bin/java /usr/bin/java
```

# 设置时区自动同步
```bash
sudo mv /etc/localtime /etc/localtime.old
sudo ln -s /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
```

# 解压 hadoop

```bash
tar -zxvf hadoop-3.3.4.tar.gz -C /export/server/
```

# 创建软连接

```bash
ln -s /export/server/hadoop-3.3.4 hadoop
```

# 配置 workers

```plaintext
node1
node2
node3
```

# 配置 hadoop-env.sh

```plaintext
export JAVA_HOME=/export/server/jdk
export HADOOP_HOME=/export/server/hadoop
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export HADOOP_LOG_DIR=${HADOOP_HOME}/logs
```

# 配置 core-site.xml
```xml
<configuration>
    <!-- 指定NameNode的地址 -->
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://node1:8020</value>
    </property>
    <!-- io操作文件缓冲区大小 -->
    <property>
        <name>io.file.buffer.size</name>
        <value>131072</value>
    </property>
</configuration>
```

# 配置 hdfs-site.xml

```xml
<configuration>
    <!-- 默认文件权限 -->
    <property>
        <name>dfs.datanode.data.dir.perm</name>
        <value>700</value>
    </property>
    <!-- NameNode元数据存储位置 -->
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/data/nn</value>
    </property>
    <!-- 允许哪几个节点的DataNode连接 -->
    <property>
        <name>dfs.namenode.hosts</name>
        <value>node1,node2,node3</value>
    </property>
    <!-- hdfm默认块大小256m -->
    <property>
        <name>dfs.blocksize</name>
        <value>268435456</value>
    </property>
    <!-- namenode处理的并发线程数 -->
    <property>
        <name>dfs.namenode.handler.count</name>
        <value>100</value>
    </property>
    <!-- DataNode元数据存储位置 -->
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/data/dn</value>
    </property>
</configuration>
```

# 分发 hadoop 文件夹
```bash
scp -r hadoop-3.3.4 node2:`pwd`/
scp -r hadoop-3.3.4 node3:`pwd`/
```

# 创建软连接
```bash
ln -s /export/server/hadoop-3.3.4 /export/server/hadoop
```

# 配置环境变量
```bash
vim /etc/profile
export HADOOP_HOME=/export/server/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
source /etc/profile
```

# 把权限改为 hadoop

```bash
chown -R hadoop:hadoop /data
chown -R hadoop:hadoop /export
```

# 格式化 namenode

```bash
su - hadoop
hadoop namenode -format
```

# 启动集群

```bash
start-dfs.sh
jps
```

# 暂停集群

```bash
stop-dfs.sh
```

# 网站
http://node1:9870

