<template>
  <el-card class="server-card" :body-style="{ padding: '0px' }">
    <!-- 其他部分保持不变 -->
    
    <div class="server-disks" v-if="server.disks && server.disks.length">
      <!-- 普通文件系统 -->
      <div v-if="normalDisks.length">
        <h4>文件系统</h4>
        <disk-bar 
          v-for="disk in normalDisks" 
          :key="disk.name" 
          :disk="disk"
        />
      </div>
      
      <!-- ZFS 存储池 -->
      <div v-if="zfsPools.length">
        <h4>ZFS 存储池</h4>
        <disk-bar 
          v-for="disk in zfsPools" 
          :key="disk.name" 
          :disk="disk"
        />
      </div>
    </div>
    
    <!-- 其他部分保持不变 -->
  </el-card>
</template>

<script>
export default {
  // ... 其他部分保持不变
  
  computed: {
    normalDisks() {
      return this.server.disks.filter(
        disk => !disk.name.startsWith('zpool-') && disk.file_system.toLowerCase() !== 'zfs'
      );
    },
    zfsPools() {
      return this.server.disks.filter(
        disk => disk.name.startsWith('zpool-')
      );
    }
  }
}
</script>

<style scoped>
.server-disks {
  padding: 15px;
}
.server-disks h4 {
  margin: 10px 0;
  color: #606266;
  font-size: 14px;
  border-bottom: 1px solid #EBEEF5;
  padding-bottom: 8px;
}
</style> 