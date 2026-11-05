# R1 的 HBase 维表（在 hbase shell 里执行：hbase shell -n < hbase_tables.rb）
# rowkey = (id % 10) + '_' + id：用「可以算出来的盐值」把连续的 id 打散到 10 个 region，避免写热点；
# 查询时 Flink 用同样的公式算出 rowkey，所以点查不受影响。按盐值预分 10 个 region。
splits = ('1'..'9').to_a
%w[dim_user dim_item].each do |table|
  if list.include?(table)
    puts "#{table} 已存在，跳过"
  else
    create table, { NAME => 'f', VERSIONS => 1 }, SPLITS => splits
    puts "#{table} 已创建"
  end
end
