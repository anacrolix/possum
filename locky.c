/*
 * by Avery Pennarun <apenwarr@gmail.com>
 *
 * This program demonstrates a bug in fcntl(F_SETLK) locking.  Test results:
 *
 * - MacOS 10.6.5 on a dual-core CPU: fails
 * - MacOS 10.4 on a single-core CPU: passes
 * - Linux 2.6.36 on a quad-core CPU: passes
 *
 */
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <assert.h>

#define NUMPROCS 20
#define NUMITERS 1000
#define WIDTH 5


static int trylock(int fd, int ofs)
{
    int rv;
    struct flock f;
    memset(&f, 0, sizeof(f));
    f.l_type = F_WRLCK;
    f.l_start = ofs;
    f.l_len = 1;
    rv = fcntl(fd, F_SETLK, &f);
    if (rv)
    {
	if (errno == EAGAIN || errno == EACCES)
	    return 0; // owned by someone else
	// else it's another kind of error
	perror("trylock");
	abort();
    }
    return 1; // locked
}


static void unlock(int fd, int ofs)
{
    int rv;
    struct flock f;
    memset(&f, 0, sizeof(f));
    f.l_type = F_UNLCK;
    f.l_start = ofs;
    f.l_len = 1;
    rv = fcntl(fd, F_SETLK, &f);
    if (rv)
    {
	perror("unlock");
	abort();
    }
}


static int check(int fd, int ofs)
{
    pid_t pid, wantpid;
    int rv;
    struct flock f;
    memset(&f, 0, sizeof(f));
    f.l_type = F_WRLCK;
    f.l_start = ofs;
    f.l_len = 1;
    wantpid = getpid();
    
    // this is a little silly: F_GETLK won't tell us if we already own the
    // lock (it'll just say "sure, you can get that lock!") so we have to
    // first fork() a subprocess and have that one check for us.  fcntl()
    // locks aren't inherited across fork(), so the subprocess *won't* be able
    // to lock if we *do* own it.
    pid = fork();
    if (pid == 0) // child
    {
	rv = fcntl(fd, F_GETLK, &f);
	if (rv)
	{
	    perror("getlock");
	    abort();
	}
	if (f.l_type == F_UNLCK)
	    _exit(0); // not locked
	else
	{
	    // owned by someone else - is it the right someone?
	    if (f.l_pid == wantpid)
		_exit(1); // yes, we own it
	    else
		_exit(0); // no, someone else owns it
	}
    }
    else if (pid > 0) // parent
    {
	int status = 0;
	wait(&status);
	assert(WIFEXITED(status));
	return WEXITSTATUS(status);
    }
    else
    {
	perror("fork");
	abort();
    }
}


static int submain(int procnum, int fd)
{
    static char owned[WIDTH];
    int i, ofs;
    
    memset(owned, 0, sizeof(owned));
    srandom(procnum);
    
    for (i = 0; i < NUMITERS; i++)
    {
	ofs = random() % WIDTH;
	if (check(fd, ofs) != owned[ofs])
	{
	    fprintf(stderr, "proc=%-5d ofs=%-5d iter=%-5d - expected %d\n",
		    procnum, ofs, i, owned[ofs]);
	    abort();
	}
	if (owned[ofs])
	{
	    unlock(fd, ofs);
	    owned[ofs] = 0;
	}
	else
	    owned[ofs] = trylock(fd, ofs);
	assert(check(fd, ofs) == owned[ofs]);
    }
    return 0;
}
    

int main()
{
    int fd, i, err;
    pid_t pid;
    
    unlink("mylock"); // in case someone else is using one from before
    fd = open("mylock", O_RDWR|O_CREAT|O_EXCL, 0600);
    
    for (i = 0; i < NUMPROCS; i++)
    {
	pid = fork();
	if (pid == 0) // child
	    _exit(submain(i, fd));
	else if (pid < 0) // error
	{
	    perror("fork");
	    exit(1);
	}
    }
    
    err = 0;
    for (i = 0; i < NUMPROCS; i++)
    {
	int status = 0;
	pid = wait(&status);
	if (status != 0)
	{
	    fprintf(stderr, "pid %ld returned %04x\n", (long)pid, status);
	    err++;
        }
    }
    
    fprintf(stderr, "Errors: %d of %d\n", err, NUMPROCS);
    return err != 0;
}
