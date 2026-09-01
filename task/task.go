package task

import (
	"context"
	"io"
	"log"
	"math"
	"os"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/google/uuid"
)

type State int

const (
	Pending State = iota
	Scheduled
	Running
	Completed
	Failed
)

type Task struct {
	ID            uuid.UUID
	ContainerID   string
	Name          string
	State         State
	Image         string
	Memory        int
	Disk          int
	ExposedPorts  nat.PortSet
	PortBindings  map[string]string
	RestartPolicy string
	StartTime     *time.Time
	FinishTime    *time.Time
}

type TaskEvent struct {
	ID        uuid.UUID
	State     State
	Timestamp time.Time
	Task      *Task
}

type Config struct {
	Name          string
	AttachStdin   bool
	AttachStdout  bool
	AttachStderr  bool
	ExposedPorts  nat.PortSet
	Cmd           []string
	Image         string
	Cpu           float64
	Memory        int64
	Disk          int64
	Env           []string
	RestartPolicy string
}

type Docker struct {
	Client *client.Client
	Config *Config
}

type DockerResult struct {
	Error       error
	Action      string
	ContainerId string
	Result      string
}

func NewDocker(c *Config) (*Docker, error) {
	dc, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}

	return &Docker{
		Client: dc,
		Config: c,
	}, nil
}

func NewConfig(t *Task) *Config {
	return &Config{
		Name:          t.Name,
		Image:         t.Image,
		Memory:        int64(t.Memory),
		Disk:          int64(t.Disk),
		ExposedPorts:  t.ExposedPorts,
		RestartPolicy: t.RestartPolicy,
	}
}

func (d *Docker) Run() DockerResult {
	ctx := context.Background()

	// Pull the Docker image.
	reader, err := d.Client.ImagePull(
		ctx,
		d.Config.Image,
		types.ImagePullOptions{},
	)
	if err != nil {
		log.Printf(
			"Error pulling image %s: %v\n",
			d.Config.Image,
			err,
		)
		return DockerResult{Error: err}
	}

	defer reader.Close()

	// Display image pull output.
	_, err = io.Copy(os.Stdout, reader)
	if err != nil {
		log.Printf("Error reading image pull output: %v\n", err)
		return DockerResult{Error: err}
	}

	// Configure restart policy.
	rp := container.RestartPolicy{
		Name: d.Config.RestartPolicy,
	}

	// Configure resources.
	r := container.Resources{
		Memory:   d.Config.Memory,
		NanoCPUs: int64(d.Config.Cpu * math.Pow(10, 9)),
	}

	// Configure the container.
	cc := container.Config{
		Image:        d.Config.Image,
		Tty:          false,
		Env:          d.Config.Env,
		Cmd:          d.Config.Cmd,
		ExposedPorts: d.Config.ExposedPorts,
	}

	// Configure the host.
	hc := container.HostConfig{
		RestartPolicy:  rp,
		Resources:      r,
		PublishAllPorts: true,
	}

	// Create the container.
	resp, err := d.Client.ContainerCreate(
		ctx,
		&cc,
		&hc,
		nil,
		nil,
		d.Config.Name,
	)
	if err != nil {
		log.Printf(
			"Error creating container using image %s: %v\n",
			d.Config.Image,
			err,
		)
		return DockerResult{Error: err}
	}

	// Start the container.
	err = d.Client.ContainerStart(
		ctx,
		resp.ID,
		types.ContainerStartOptions{},
	)
	if err != nil {
		log.Printf(
			"Error starting container %s: %v\n",
			resp.ID,
			err,
		)
		return DockerResult{Error: err}
	}

	// Get container logs.
	out, err := d.Client.ContainerLogs(
		ctx,
		resp.ID,
		types.ContainerLogsOptions{
			ShowStdout: true,
			ShowStderr: true,
		},
	)
	if err != nil {
		log.Printf(
			"Error getting logs for container %s: %v\n",
			resp.ID,
			err,
		)
		return DockerResult{Error: err}
	}

	defer out.Close()

	// Copy container logs to stdout/stderr.
	_, err = stdcopy.StdCopy(
		os.Stdout,
		os.Stderr,
		out,
	)
	if err != nil {
		log.Printf(
			"Error copying container logs: %v\n",
			err,
		)
		return DockerResult{Error: err}
	}

	return DockerResult{
		ContainerId: resp.ID,
		Action:      "start",
		Result:      "success",
	}
}

func (d *Docker) Stop(id string) DockerResult {
	log.Printf("Attempting to stop container %v", id)

	ctx := context.Background()

	// Stop the container.
	err := d.Client.ContainerStop(
		ctx,
		id,
		nil,
	)
	if err != nil {
		log.Printf(
			"Error stopping container %s: %v\n",
			id,
			err,
		)
		return DockerResult{Error: err}
	}

	// Remove the container.
	err = d.Client.ContainerRemove(
		ctx,
		id,
		types.ContainerRemoveOptions{
			RemoveVolumes: true,
			RemoveLinks:   false,
			Force:         false,
		},
	)
	if err != nil {
		log.Printf(
			"Error removing container %s: %v\n",
			id,
			err,
		)
		return DockerResult{Error: err}
	}

	return DockerResult{
		Action: "stop",
		Result: "success",
		Error:  nil,
	}
}