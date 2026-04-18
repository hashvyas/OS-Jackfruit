# OS-Jackfruit: Multi-Container Runtime

A lightweight Linux container runtime in C with a long-running supervisor and a kernel-space memory monitor.

## 1. Team Information

- **Name 1:** V Vyas, **SRN:** PES2UG24CS569
- **Name 2:** Thejaswi S, **SRN:** PES2UG24CS562

---

## 2. Build, Load, and Run Instructions

### Prerequisites (Fresh Ubuntu 22.04/24.04 VM)

```bash
sudo apt update
sudo apt install -y build-essential wget tar linux-headers-$(uname -r)
```

### Download Alpine Base Rootfs

```bash
cd ~/OS-Jackfruit-main
wget https://dl-cdn.alpinelinux.org/alpine/v3.19/releases/x86_64/alpine-minirootfs-3.19.1-x86_64.tar.gz
mkdir -p rootfs-base
sudo tar -xzf alpine-minirootfs-3.19.1-x86_64.tar.gz -C rootfs-base
```

### Building the Project

```bash
cd ~/OS-Jackfruit-main
make -C boilerplate
```

This builds:
- `engine` — The container runtime (supervisor + CLI)
- `cpu_hog` — CPU-bound workload for scheduling experiments
- `memory_hog` — Memory allocation workload
- `io_pulse` — I/O-bound workload
- `monitor.ko` — Kernel module for memory monitoring

### Loading the Kernel Module

```bash
sudo insmod boilerplate/monitor.ko
ls -l /dev/container_monitor
```

### Preparing Root Filesystems

```bash
cd ~/OS-Jackfruit-main

sudo cp -a rootfs-base rootfs-alpha
sudo cp -a rootfs-base rootfs-beta

sudo cp boilerplate/cpu_hog boilerplate/memory_hog boilerplate/io_pulse ./rootfs-alpha/
sudo cp boilerplate/cpu_hog boilerplate/memory_hog boilerplate/io_pulse ./rootfs-beta/
```

### Running the Supervisor (Terminal 1)

```bash
cd ~/OS-Jackfruit-main/boilerplate
sudo ./engine supervisor ../rootfs-base
```

### CLI Commands (Terminal 2)

```bash
cd ~/OS-Jackfruit-main/boilerplate

# Start a container in background
sudo ./engine start alpha ../rootfs-alpha /cpu_hog

# Start a container and wait for it to exit
sudo ./engine run alpha ../rootfs-alpha /cpu_hog

# List tracked containers
sudo ./engine ps

# View container logs
sudo ./engine logs alpha

# Stop a running container
sudo ./engine stop alpha
```

### With Memory Limits

```bash
sudo ./engine start alpha ../rootfs-alpha /memory_hog --soft-mib 48 --hard-mib 80
```

### With Nice Value (Scheduling)

```bash
sudo ./engine start alpha ../rootfs-alpha /cpu_hog --nice -10
```

### Running Scheduling Experiments

```bash
# Experiment A: CPU vs CPU with different priorities
sudo ./engine start high_prio ../rootfs-alpha /cpu_hog --nice -20
sudo ./engine start low_prio ../rootfs-beta /cpu_hog --nice 19

# Experiment B: CPU-bound vs I/O-bound at same priority
sudo ./engine start cpu_work ../rootfs-alpha /cpu_hog --nice 0
sudo ./engine start io_work ../rootfs-beta /io_pulse --nice 0
```

### Clean Up

```bash
# Stop supervisor with Ctrl+C in Terminal 1
# Then in Terminal 2:
sudo rmmod monitor
```
