#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#include "util.h"

size_t getMemUsage(){
	pid_t pid = getpid();

	char cmd[1000];

	sprintf(cmd, "cat /proc/%d/status | grep VmData |  awk '{print $2}'", pid);


	FILE *f = popen(cmd, "r");

	int dataSize;
	
	fscanf(f, "%d\n", &dataSize);

	pclose(f);

	return (size_t)dataSize * (size_t)1024;
}
