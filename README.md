# HBTree

HBTree是一个基于DRAM-NVM混合存储架构的树形索引结构。HBTree整体上可以看作是由一个大的B+树按照某一层进行划分而来，划分后底层存在大量高度相同、key范围不重叠的子树，将热度高的子树缓存到DRAM中，通过日志保证其写请求数据的可靠性；热度较低的子树存在NVM中，直接进行修改和访问；顶层的中间节点全部位于DRAM中。

## 代码解析

+ fastfair目录中：FAST&FAIR方案（libpmem版本）
+ fastfairobj目录中：FAST&FAIR方案（libpmemobj版本），作者开源实现的版本
+ fptree目录：FPTree方案，基于FPTree论文以及FAS&FAIR方案的代码修改实现，未实现ulog
+ src目录：包含了hbtree的主要代码

## 编译

建议看懂某个方案目录下的Makefile文件，所有方案的Makefile文件都差不多。
例如，在src目录下面 ``` make hbtree ``` 即可编译hbtree的测试程序；在src目录下面``` make install && make install-headers ``` 即可生成hbtreee的静态链接库以及头文件，默认输出到 ```/usr/local/include``` 和 ``` /usr/local/lib``` 下面。

## 运行
### YCSB测试
在目录下直接```make ycsb```，然后```numactl --cpubind=1 ./ycsb -db dbname -P workloadpath```即可。

## 注意
为了便于测试，代码中NVM文件的路径都已经写死```/mnt/pmem1```，测试前需要手动修改所有路径。
运行时出现段错误，可能是NVM空间不足或历史数据文件未删除。
