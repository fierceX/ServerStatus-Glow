<template>
  <div class="disk-bar">
    <div class="disk-name">
      <span :class="{ 'zfs-pool': isZFSPool }">
        {{ displayName }}
        <span class="mount-point">({{ disk.mount_point }})</span>
      </span>
    </div>
    <div class="progress-wrap">
      <el-progress 
        :percentage="usagePercent" 
        :format="formatUsage"
        :status="getProgressStatus(usagePercent)"
      />
    </div>
  </div>
</template>

<script>
export default {
  props: {
    disk: {
      type: Object,
      required: true
    }
  },
  computed: {
    isZFSPool() {
      return this.disk.name.startsWith('zpool-');
    },
    displayName() {
      return this.isZFSPool ? this.disk.name.replace('zpool-', '') : this.disk.name;
    },
    usagePercent() {
      return Math.round((this.disk.used / this.disk.total) * 100);
    }
  },
  methods: {
    formatUsage(percent) {
      return `${this.formatBytes(this.disk.used)} / ${this.formatBytes(this.disk.total)} (${percent}%)`;
    },
    formatBytes(bytes) {
      // 保持原有的格式化代码...
    },
    getProgressStatus(percent) {
      if (percent >= 90) return 'exception';
      if (percent >= 70) return 'warning';
      return 'success';
    }
  }
}
</script>

<style scoped>
.disk-bar {
  margin-bottom: 10px;
}
.disk-name {
  margin-bottom: 5px;
  font-size: 14px;
}
.zfs-pool {
  font-weight: bold;
  color: #409EFF;
}
.mount-point {
  color: #909399;
  font-size: 12px;
}
.progress-wrap {
  margin-bottom: 15px;
}
</style> 