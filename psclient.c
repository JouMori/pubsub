#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <csse2310a3.h>
#include <pthread.h>

#define MIN_ARGS 3
#define TOPIC_PRESENT 4
#define FIRST_TOPIC 3
#define PORT 1
#define NAME 2

/* check_spaces_colons_newlines_empty()
 * ------------------------------------
 * Checks a given string for the presence of spaces, colons and newlines, and 
 * whether it is empty.
 *
 * str: the string to check
 */
int check_spaces_colons_newlines_empty(char* str) {
    if (strchr(str, ' ') != NULL || 
	    strchr(str, ':') != NULL ||
	    strchr(str, '\n') != NULL || 
	    !strcmp(str, "")) {
	return 0;
    }
    return 1;
}

/* handle_arguments()
 * ------------------
 * Iterates through the command line arguments, ensuring all conform to the 
 * required standards. 
 *
 * argc: the number of command line arguments
 * argv: the array containing the command line arguments
 *
 * Errors: the program will exit with status 1 if there are insufficient 
 * command line arguments. The program will exit with status 2 if the name
 * argument is invalid or any of the topic arguments are invalid.
 */
void handle_arguments(int argc, char* argv[]) {
    // Too few command line arguments
    if (argc < MIN_ARGS) {
	fprintf(stderr, "Usage: psclient portnum name [topic] ...\n");
	exit(1);
    }

    // Invalid name
    if (!check_spaces_colons_newlines_empty(argv[NAME])) {
	fprintf(stderr, "psclient: invalid name\n");
	exit(2);
    }

    // Invalid topic
    if (argc >= TOPIC_PRESENT) {
    	for (int i = FIRST_TOPIC; i < argc; i++) {
	    if (!check_spaces_colons_newlines_empty(argv[i])) {
		fprintf(stderr, "psclient: invalid topic\n");
		exit(2);
	    }
	}
    }
}

/* port_error()
 * ------------
 * Prints the port error message to standard error, flushes, and exits the 
 * program with status 3.
 *
 * port: the port number to print
 */
void port_error(char* port) {
    fprintf(stderr, "psclient: unable to connect to port %s\n", port);
    fflush(stderr);
    exit(3);
}

/* connect_to_port()
 * -----------------
 * Connects to a given port number.
 *
 * port: the port number to connect to
 *
 * Returns: the connected socket file descriptor
 * Errors: the program will exit with status 3 if the address could not be
 * determined, or there was an error connecting to the socket
 * Reference: this code is taken from the Week 10 "net2.c" lecture example
 */
int connect_to_port(char* port) {
    struct addrinfo* ai = 0;
    struct addrinfo hints;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    // Could not determine address
    int err;
    if ((err = getaddrinfo("localhost", port, &hints, &ai))) {
	port_error(port);
    }

    // Create socket
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(fd, (struct sockaddr*) ai->ai_addr, sizeof(struct sockaddr))) {
	port_error(port); // Error connecting to socket
    }

    return fd;
}

/* read_thread()
 * -------------
 * Thread handling function responsible for reading from the connected socket. 
 * Repeatedly reads single lines from the socket, prints them to standard out,
 * and flushes. Upon disconnection from the socket, prints a message to 
 * standard error and exits with status 4.
 *
 * arg: the argument passed when creating the thread, in this case the file
 * pointer of the socket
 */
void* read_thread(void* arg) {
    FILE* from = (FILE*) arg;
    char* line;

    // Read from server
    while ((line = read_line(from)) != NULL) {
	printf("%s\n", line);
	fflush(stdout);
    }

    // Connection to server closed
    fprintf(stderr, "psclient: server connection terminated\n");
    exit(4);
}

int main(int argc, char* argv[]) {
    handle_arguments(argc, argv);
    char* port = argv[PORT];
    char* name = argv[NAME];

    // Connect to port and open file pointers for read/write
    int fd = connect_to_port(port);
    int fd2 = dup(fd);
    FILE* to = fdopen(fd, "w");
    FILE* from = fdopen(fd2, "r");

    // Send name to server
    fprintf(to, "name %s\n", name);
    fflush(to);

    // Send subscription requests to server
    if (argc >= TOPIC_PRESENT) {
	for (int i = FIRST_TOPIC; i < argc; i++) {
	    fprintf(to, "sub %s\n", argv[i]);
	    fflush(to);
	}
    }

    // Create thread to read from server
    pthread_t tid;
    pthread_create(&tid, 0, read_thread, from); 
    pthread_detach(tid);

    // Read from stdin 
    char* line;
    while ((line = read_line(stdin)) != NULL) {
	fprintf(to, "%s\n", line);
	fflush(to);
    }

    exit(0);
}
