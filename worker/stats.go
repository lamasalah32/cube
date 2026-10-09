
package worker

import (
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
)

type Stats struct {
	MemTotal       uint64  `json:"mem_total"`
	MemAvailable   uint64  `json:"mem_available"`
	MemUsed        uint64  `json:"mem_used"`
	MemUsedPercent float64 `json:"mem_used_percent"`

	DiskTotal       uint64  `json:"disk_total"`
	DiskFree        uint64  `json:"disk_free"`
	DiskUsed        uint64  `json:"disk_used"`
	DiskUsedPercent float64 `json:"disk_used_percent"`

	CpuUsedPercent float64 `json:"cpu_used_percent"`

	Load1  float64 `json:"load1"`
	Load5  float64 `json:"load5"`
	Load15 float64 `json:"load15"`

	TaskCount int
}

func GetStats() (*Stats, error) {
	result := &Stats{}

	// Memory
	memory, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}

	result.MemTotal = memory.Total
	result.MemAvailable = memory.Available
	result.MemUsed = memory.Used
	result.MemUsedPercent = memory.UsedPercent

	// Disk: root filesystem
	diskInfo, err := disk.Usage("/")
	if err != nil {
		return nil, err
	}

	result.DiskTotal = diskInfo.Total
	result.DiskFree = diskInfo.Free
	result.DiskUsed = diskInfo.Used
	result.DiskUsedPercent = diskInfo.UsedPercent

	// CPU: measure over one second
	cpuPercent, err := cpu.Percent(1, false)
	if err != nil {
		return nil, err
	}

	if len(cpuPercent) > 0 {
		result.CpuUsedPercent = cpuPercent[0]
	}

	// Load average
	loadInfo, err := load.Avg()
	if err == nil {
		result.Load1 = loadInfo.Load1
		result.Load5 = loadInfo.Load5
		result.Load15 = loadInfo.Load15
	}

	return result, nil
}