import json

shard_num = 20      # 分片数 (-S)
node_num = 4     # 每分片节点数 (-N)
start_port = 8000  # 起始端口号

ip_table = {}

# 1. 为普通共识节点分配端口
for s in range(shard_num):
    ip_table[str(s)] = {}
    for n in range(node_num):
        # 本地测试都使用 127.0.0.1，端口号递增
        ip_table[str(s)][str(n)] = f"127.0.0.1:{start_port}"
        start_port += 1

# 2. 为 Supervisor 分配端口 (分片 ID 固定为 "12345")
# 预留一个与其他节点不冲突的端口，比如使用当前 start_port 或者固定的 18888
ip_table["2147483647"] = {
    "0": "127.0.0.1:18888"
}

# 3. 写入 ipTable.json
with open("ipTable.json", "w", encoding="utf-8") as f:
    json.dump(ip_table, f, indent=4)

print(f"成功生成 ipTable.json！共包含 {shard_num * node_num} 个普通节点和 1 个 Supervisor。")