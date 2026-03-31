#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PROC_NET_DEV "/proc/net/dev"
#define NETNS_DIR "/var/run/netns"

struct namespace_guard {
    int original_fd;
};

static int enter_namespace(const char *namespace, struct namespace_guard *guard) {
    char target_path[PATH_MAX];
    int target_fd;

    guard->original_fd = open("/proc/self/ns/net", O_RDONLY);
    if (guard->original_fd < 0) {
        fprintf(stderr, "failed to open current netns: %s\n", strerror(errno));
        return -1;
    }

    snprintf(target_path, sizeof(target_path), "%s/%s", NETNS_DIR, namespace);
    target_fd = open(target_path, O_RDONLY);
    if (target_fd < 0) {
        fprintf(stderr, "failed to open target netns %s: %s\n", target_path, strerror(errno));
        close(guard->original_fd);
        guard->original_fd = -1;
        return -1;
    }

    if (setns(target_fd, CLONE_NEWNET) != 0) {
        fprintf(stderr, "setns(%s) failed: %s\n", namespace, strerror(errno));
        close(target_fd);
        close(guard->original_fd);
        guard->original_fd = -1;
        return -1;
    }

    close(target_fd);
    return 0;
}

static void leave_namespace(struct namespace_guard *guard) {
    if (guard->original_fd < 0) {
        return;
    }

    if (setns(guard->original_fd, CLONE_NEWNET) != 0) {
        fprintf(stderr, "failed to restore original netns: %s\n", strerror(errno));
    }

    close(guard->original_fd);
    guard->original_fd = -1;
}

static int print_proc_net_dev(const char *label) {
    FILE *fp;
    char line[4096];

    fp = fopen(PROC_NET_DEV, "r");
    if (fp == NULL) {
        fprintf(stderr, "failed to open %s for %s: %s\n", PROC_NET_DEV, label, strerror(errno));
        return -1;
    }

    printf("===== namespace: %s =====\n", label);
    while (fgets(line, sizeof(line), fp) != NULL) {
        fputs(line, stdout);
    }
    printf("\n");

    fclose(fp);
    return 0;
}

int main(int argc, char **argv) {
    struct namespace_guard guard = { .original_fd = -1 };
    const char *namespace;

    if (argc != 2) {
        fprintf(stderr, "usage: %s <namespace>\n", argv[0]);
        fprintf(stderr, "example: %s ns1\n", argv[0]);
        return 1;
    }

    namespace = argv[1];

    if (print_proc_net_dev("default-before") != 0) {
        return 1;
    }

    if (enter_namespace(namespace, &guard) != 0) {
        return 1;
    }

    if (print_proc_net_dev(namespace) != 0) {
        leave_namespace(&guard);
        return 1;
    }

    leave_namespace(&guard);

    if (print_proc_net_dev("default-after") != 0) {
        return 1;
    }

    return 0;
}