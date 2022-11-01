#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <math.h>
#include <signal.h>
#include <dirent.h>
#include <pthread.h>
#include <sys/stat.h>

#include <curl/curl.h>

#define print_error_line(format, args...) do { \
    fprintf(stderr, "%s:%d in %s: Error: ", __FILE__, __LINE__, __FUNCTION__); \
    fprintf(stderr, format, ##args); \
    fprintf(stderr, "\n"); \
} while (0)

#define print_error_line_q(format, args...) do { \
    fprintf(stderr, "%s:%d in %s: Error: ", __FILE__, __LINE__, __FUNCTION__); \
    fprintf(stderr, format, ##args); \
    fprintf(stderr, "\n"); \
    exit(EXIT_FAILURE); \
} while (0)

int curl_initialized = 0;

#define INITIAL_CONTENT_SIZE 1
#define DEFAULT_MEMORY_PER_THREAD 2048

char filename[PATH_MAX + 1];
char *input_content = NULL; // A combination of all input files. Newlines have been replaced by '\0'
size_t bytes_content = 0; // The amount of bytes containes in input_content
size_t allocated_content = INITIAL_CONTENT_SIZE; // The amount of bytes allocated for input_content
char **url_list = NULL; // A list of pointers to the URLs in input_content
int n_urls = 0; // The amount of pointers in url_list
int i_urls = 0; // The current url index
pthread_mutex_t mutex_urls; // The mutex for i_urls

typedef struct {
    int i;
    pthread_t pthread;
    char filename[NAME_MAX + 1];
    char *memory;
    size_t bytes_memory;
    size_t allocated_memory;
} download_thread_t;
download_thread_t *download_threads = NULL;

typedef struct {
    int parsed;
    char name[64];
    char abbreviation[8];
    int requires_argument;
    int received_argument;
    char argument[64];
} option_t;
option_t ALL_OPTIONS[] = {
                0, "threads", "j", 1, 0, "",
                0, "output", "o", 1, 0, "",
                0, "file-existence-behaviour", "b", 1, 0, ""
};

#define THREADS_MIN (1)
#define THREADS_MAX (4096)

#define DEFAULT_OUTPUT_DIR "curl-downloader-output"

// The options have to fit their index in ALL_OPTIONS
enum options {
    OPTION_THREADS = 0,
    OPTION_OUTPUT = 1,
    OPTION_FILE_EXISTENCE_BEHAVIOUR = 2
};

int n_threads = 4; // threads
char output[PATH_MAX + 1] = DEFAULT_OUTPUT_DIR; // output

// file-existence-behaviour
enum FILE_EXISTENCE_BEHAVIOURS {
    FEB_UNDEFINED = -1,
    FEB_APPEND_TIMESTAMP = 0,
    FEB_APPEND_NUMBER = 1
};
enum FILE_EXISTENCE_BEHAVIOURS file_existence_behaviour = FEB_UNDEFINED;

unsigned long nanos(int mode) {
    struct timespec spec;
    if (clock_gettime(mode, &spec) == -1) return 0;
    return spec.tv_sec * 1000000000L + spec.tv_nsec;
}

option_t *find_option(char *input) {
    for (int i = 0; i < sizeof ALL_OPTIONS / sizeof *ALL_OPTIONS; i++) {
        option_t *option = ALL_OPTIONS + i;
        if (strcmp(input, option->name) == 0) return option;
    }
    return NULL;
}

option_t *find_option_by_abbr(char input) {
    for (int i = 0; i < sizeof ALL_OPTIONS / sizeof *ALL_OPTIONS; i++) {
        option_t *option = ALL_OPTIONS + i;
        if (input == option->abbreviation[0]) return option;
    }
    return NULL;
}

size_t download_callback_f(void *content, size_t size, size_t nmemb, void *data) {
    download_thread_t *download_thread = (download_thread_t *) data;

    size_t total_size = size * nmemb;

    while (download_thread->bytes_memory + total_size + 1 > download_thread->allocated_memory) {
        size_t new_allocated = (size_t) ceil((double) (download_thread->allocated_memory + total_size + 1) * 1.8);
        char *new_ptr = realloc(download_thread->memory, new_allocated);
        if (new_ptr == NULL) print_error_line_q("Error %d (%s) while trying to reallocate memory", errno, strerror(errno));
        download_thread->memory = new_ptr;
        download_thread->allocated_memory = new_allocated;
    }

    memcpy(download_thread->memory + download_thread->bytes_memory, content, total_size);
    download_thread->bytes_memory += total_size;
    download_thread->memory[download_thread->bytes_memory] = '\0';

    return total_size;
}

size_t header_callback_f(void *header, size_t size, size_t nitems, void *data) {
    download_thread_t *download_thread = (download_thread_t *) data;

    size_t total_size = size * nitems;

    // Check for the content-disposition header
    if (strncmp((char *) header, "content-disposition: ", total_size < 21 ? total_size : 21) == 0) {
        char *current = header + 21, // + 21 to skip "content-disposition: "
             *end_ptr = header + total_size;
        char *filename_start = NULL;
        size_t filename_len = 0;

        // Find the filename
        for (; current < end_ptr; current++) {
            if (filename_start != NULL && *current == ',') { // If a ',' is found, we know the upper filename boundary
                filename_len = current - filename_start;
                break;
            } else if (filename_start == NULL && strncmp(current, "filename=", end_ptr - current < 9 ? end_ptr - current : 9) == 0) { // If "filename=" is found, mark the lower filename boundary
                filename_start = current + 9; // + 9 to skip "filename="
            }
        }

        if (filename_start == NULL) goto return_statement;

        // In case no upper filename boundary was found
        if (filename_len == 0) {
            filename_len = end_ptr - filename_start;
        }

        // Check if name is too long
        if (filename_len > sizeof download_thread->filename - 1) {
            filename_len = sizeof download_thread->filename - 1;
        }

        // Copy the correct amount of characters to the download thread
        strncpy(download_thread->filename, filename_start, filename_len);
        printf("Extracted filename from content-disposition: <%s>\n", download_thread->filename);
    }

    return_statement:
    return total_size;
}

void *download_thread_f(void *data) {
    download_thread_t *download_thread = (download_thread_t *) data;

    // Initialize CURL
    CURL *curl = curl_easy_init();
    curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, "");
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, download_callback_f); // Data callback
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) download_thread);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, header_callback_f); // Header callback. Needed for filename extraction
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, (void *) download_thread);
    if (curl == NULL) print_error_line_q("Error while trying initialize download thread with curl_easy_init()");

    // Enter an infinite loop until all URLs have been fetched
    while (1) {
        pthread_mutex_lock(&mutex_urls); // The url index is accessed by all threads

        // Check if all URLs have been fetched
        if (i_urls >= n_urls) {
            pthread_mutex_unlock(&mutex_urls);
            break;
        }

        // Reallocate memory and reset download info
        char *new_ptr = realloc(download_thread->memory, DEFAULT_MEMORY_PER_THREAD);
        if (new_ptr == NULL) print_error_line_q("Error %d (%s) while trying to reallocate memory", errno, strerror(errno));
        download_thread->memory = new_ptr;
        download_thread->allocated_memory = DEFAULT_MEMORY_PER_THREAD;
        download_thread->bytes_memory = 0; // Reset memory
        download_thread->filename[0] = '\0'; // Reset filename

        char *url = url_list[i_urls++]; // Fetch the next URL
        pthread_mutex_unlock(&mutex_urls); // Done accessing url index. Release the mutex

        printf("Fetched %s\n", url);
        curl_easy_setopt(curl, CURLOPT_URL, url);
        CURLcode error = curl_easy_perform(curl);
        if (error != CURLE_OK) {
            print_error_line("CURL Error %d (%s) while trying to run curl_easy_perform() on URL: %s", error, curl_easy_strerror(error), url);
        } else {
            // Check if the filename was given in a "content-disposition" header
            if (download_thread->filename[0] == '\0') {
                // Extract the filename from the URL
                char *current = url, *end_ptr = url + strlen(url);
                char *filename_start = NULL;
                size_t filename_len = 0;

                for (; current < end_ptr; current++) {
                    if (*current == '/' && current[1] != '\0') { // Find the last appearing '/' character
                        filename_start = current + 1; // Mark the lower filename boundary
                    } else if (*current == '?' && filename_start != NULL) { // If a '?' is found, mark the upper filename boundary
                        filename_len = current - filename_start;
                        break;
                    }
                }

                if (filename_start != NULL) {
                    // Check if filename length is too small or too large
                    if (filename_len == 0 || filename_len > sizeof download_thread->filename - 1) filename_len = sizeof download_thread->filename - 1;
                    strncpy(download_thread->filename, filename_start, filename_len);
                } else { // No filename was found
                    print_error_line("Failed to extract filename from URL: %s", url);
                    snprintf(download_thread->filename, sizeof download_thread->filename - 1,
                             "unknown-filename-%lu",
                             nanos(CLOCK_REALTIME)
                    );
                }
            }

            char default_filename[PATH_MAX + 1];
            char final_filename[PATH_MAX + 1];

            strncpy(default_filename, output, sizeof default_filename - 1);
            strncat(default_filename, "/", sizeof default_filename - 1);
            strncat(default_filename, download_thread->filename, sizeof default_filename - 1);

            errno = 0;
            int access_err = access(default_filename, R_OK);
            if (access_err != 0 && errno == 2) {
                // File doesn't exist. This is the preferred option
                strncpy(final_filename, default_filename, sizeof final_filename - 1);
            } else if (access_err != 0 && errno != 2) {
                // File already exists but no permission or another error occurred. This is a fatal error
                print_error_line_q("Error %d (%s) while trying to access: %s", errno, strerror(errno), download_thread->filename);
            } else if (access_err == 0) {
                // File already exists and we have permission. Attach timestamp to filename

                size_t str_len = strlen(download_thread->filename);
                char *filetype = NULL;
                char *current = download_thread->filename;
                for (; *current != '\0'; current++) {
                    if (*current == '.') filetype = current;
                }
                if (filetype == NULL) filetype = download_thread->filename + str_len; // Will point to '\0'
                size_t filetype_len = strlen(filetype);
                size_t filename_len = str_len - filetype_len;

                snprintf(final_filename, sizeof final_filename - 1, "%s/%.*s-%lu%s",
                         output,
                         (int) filename_len,
                         download_thread->filename,
                         nanos(CLOCK_REALTIME),
                         filetype
                );
            }

            printf("Final filename: <%s>\n", final_filename);

            int fd = open(final_filename, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
            if (fd < 0) print_error_line("Error %d (%s) while trying to open: %s", errno, strerror(errno), final_filename);

            ssize_t bytes_written = write(fd, download_thread->memory, download_thread->bytes_memory);
            if (bytes_written < 0) print_error_line("Error %d (%s) while trying to write to: %s", errno, strerror(errno), final_filename);
            if (bytes_written != download_thread->bytes_memory) print_error_line("bytes_written (%ld) != download_thread->bytes_memory (%lu)", bytes_written, download_thread->bytes_memory);
            close(fd);
        }
    }

    // All URLs have been fetched. Clean up and end the thread
    curl_easy_cleanup(curl);
    return NULL;
}

void add_input_file(char *path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        print_error_line("Error %d (%s) while trying to add input file %s", errno, strerror(errno), path);
        return;
    }

    size_t total_read = 0;
    ssize_t current_read;
    char buffer[2048];
    size_t allocated = sizeof buffer + 2; // optional '\n' and necessary '\0'
    char *file_contents = malloc(allocated);
    if (file_contents == NULL) print_error_line_q("Error %d (%s) while trying to allocate memory", errno, strerror(errno));

    // Read from file
    do {
        current_read = read(fd, buffer, sizeof buffer);
        if (current_read < 0) {
            print_error_line("Error %d (%s) while trying to read from input file %s", errno, strerror(errno), path);
            close(fd);
            return;
        }

        while (total_read + current_read + 2 > allocated) { // +2 for optional '\n' and necessary '\0' terminator
            size_t new_allocated = (size_t) ceil((double) allocated * 1.8 + 1);
            char *new_ptr = realloc(file_contents, new_allocated);
            if (new_ptr == NULL) print_error_line_q("Error %d (%s) while trying to reallocate memory", errno, strerror(errno));
            file_contents = new_ptr;
            allocated = new_allocated;
        }

        memcpy(file_contents + total_read, buffer, current_read);
        total_read += current_read;
    } while (current_read > 0);
    close(fd);

    // If the file doesn't end with a newline, append one
    if (file_contents[total_read - 1] != '\n') {
        file_contents[total_read] = '\n';
        file_contents[total_read + 1] = '\0';
        total_read++;
    } else {
        file_contents[total_read] = '\0';
    }

    // Copy the file content to the global input_content variable
    while (bytes_content + total_read + 1 > allocated_content) {
        size_t new_allocated = (size_t) ceil((double) allocated_content * 1.8 + 1);
        char *new_ptr = realloc(input_content, new_allocated);
        if (new_ptr == NULL) print_error_line_q("Error %d (%s) while trying to reallocate memory", errno, strerror(errno));
        input_content = new_ptr;
        allocated_content = new_allocated;
    }

    memcpy(input_content + bytes_content, file_contents, total_read);
    bytes_content += total_read;
    input_content[bytes_content] = '\0';
}

void parse_input_content() {
    // Count the amount of newlines
    char *current = input_content;
    int n = 0;
    for (; *current != '\0'; current++) {
        if (*current == '\n') n++;
    }

    // The amount of newlines equals the amount of URLs
    url_list = malloc(n * sizeof *url_list);
    n_urls = n;

    int i = 0;
    current = input_content;
    char *url_start = input_content;
    for (; *current != '\0'; current++) {
        if (*current == '\n') {
            *current = '\0';
            url_list[i++] = url_start;
            url_start = current + 1;
        }
    }

    printf("Added %d URL%s\n", n_urls, n_urls == 1 ? "" : "s");
}

void initialize(int argc, char **argv) {
    option_t *options[argc];
    char *args[argc];
    int n_options = 0, n_args = 0;

    int i = 0;
    while (++i < argc) {
        char *argument = argv[i];
        switch (*argument) {
            case '-': { // Parse option, either by full name or its abbreviation
                option_t *option = NULL;
                char *current = argument;
                if (current[1] == '-') { // Parse by full name
                    option = find_option(current + 2); // Shift to the start of the full name
                    if (option == NULL) {
                        goto unrecognized_option;
                    } else {
                        if (option->parsed) goto already_parsed;
                        option->parsed = 1;
                        options[n_options++] = option;
                    }
                } else { // Parse options via their abbreviations
                    while (*++current != '\0') { // Parse as many options as possible
                        option = find_option_by_abbr(*current);
                        if (option == NULL) {
                            goto unrecognized_option;
                        } else {
                            if (option->parsed) goto already_parsed;
                            option->parsed = 1;
                            options[n_options++] = option;
                        }
                    }
                }
                break;

                already_parsed:
                fprintf(stderr, "Parsing Error: Already parsed option: %s\n", option->name);
                exit(EXIT_FAILURE);

                unrecognized_option:
                fprintf(stderr, "Parsing Error: Unrecognized option: %c\n", *current);
                exit(EXIT_FAILURE);
            }
            default: {
                option_t *current_option = n_options > 0 ? options[n_options - 1] : NULL;
                if (current_option != NULL && current_option->requires_argument && !current_option->received_argument) {
                    strncpy(current_option->argument, argument, sizeof current_option->argument - 1);
                    current_option->received_argument = 1;
                } else {
                    args[n_args++] = argument;
                }
                break;
            }
        }
    }

    int failed = 0;
    for (i = 0; i < n_options; i++) {
        if (options[i]->requires_argument && !options[i]->received_argument) {
            fprintf(stderr, "Parsing Error: Missing required argument for option: %s\n", options[i]->name);
            failed = 1;
        }
    }
    if (failed) exit(EXIT_FAILURE);

    printf("Options (%d): ", n_options);
    for (i = 0; i < n_options; i++) {
        if (options[i]->requires_argument) {
            printf("<%s:%s>", options[i]->name, options[i]->argument);
        } else {
            printf("<%s>", options[i]->name);
        }
        if (i + 1 < n_options) printf(", ");
    }
    printf("\n");

    printf("Arguments (%d): ", n_args);
    for (i = 0; i < n_args; i++) {
        printf("<%s>", args[i]);
        if (i + 1 < n_args) printf(", ");
    }
    printf("\n");

    // Done with syntax parsing. Now parse semantics

    if (n_args < 1) {
        fprintf(stderr, "Correct usage: %s [OPTIONS] FILE/DIRECTORY\n", argv[0]);
        fprintf(stderr, "Input Error: Missing required FILE/DIRECTORY.\n");
        exit(EXIT_FAILURE);
    }

    strncpy(filename, args[0], sizeof filename - 1);
    if (access(filename, R_OK) != 0) print_error_line_q("Error %d (%s) while trying to access %s", errno, strerror(errno), filename);

    // Parse options

    if (ALL_OPTIONS[OPTION_THREADS].parsed) {
        char *input = ALL_OPTIONS[OPTION_THREADS].argument;

        char *endptr;
        errno = 0;
        int value = (int) strtol(input, &endptr, 10);
        if (errno != 0 || endptr == input) {
            printf("Invalid thread argument: %s\n", input);
            exit(EXIT_FAILURE);
        }

        if (value < THREADS_MIN) {
            printf("Invalid thread argument: %d < %d (THREADS_MIN)\n", value, THREADS_MIN);
            exit(EXIT_FAILURE);
        } else if (value > THREADS_MAX) {
            printf("Invalid thread argument: %d > %d (THREADS_MAX)\n", value, THREADS_MAX);
            exit(EXIT_FAILURE);
        }
        n_threads = value;
        printf("Using n_threads = %d\n", n_threads);
    }

    if (ALL_OPTIONS[OPTION_OUTPUT].parsed) {
        char *value = ALL_OPTIONS[OPTION_OUTPUT].argument;

        strncpy(output, value, sizeof output - 1);
        printf("Using output = %s\n", output);
    }

    if (ALL_OPTIONS[OPTION_FILE_EXISTENCE_BEHAVIOUR].parsed) {
        char *input = ALL_OPTIONS[OPTION_FILE_EXISTENCE_BEHAVIOUR].argument;

        if (strcmp(input, "append-timestamp") == 0 ||strcmp(input, "at") == 0) {
            file_existence_behaviour = FEB_APPEND_TIMESTAMP;
        } else if (strcmp(input, "append-number") == 0 || strcmp(input, "an") == 0) {
            file_existence_behaviour = FEB_APPEND_NUMBER;
        } else {
            printf("Unknown file existence behaviour argument: %s\n", input);
            exit(EXIT_FAILURE);
        }
    }

    // Done with semantics parsing. Now initialize the actual threads etc.

    // Check if CURL is threadsafe
    curl_version_info_data *info = curl_version_info(CURLVERSION_NOW);
    if (!(info->features & CURL_VERSION_THREADSAFE)) {
        print_error_line_q("This CURL version (%s) is not thread safe!\n", info->version);
    }

    // Initialize CURL
    CURLcode res_init = curl_global_init(CURL_GLOBAL_ALL);
    if (res_init != CURLE_OK) print_error_line_q("Error %d (%s) while trying to initialize CURL", res_init, curl_easy_strerror(res_init));
    curl_initialized = 1;

    // CURL is verified and initialized. Start by parsing input data

    input_content = malloc(INITIAL_CONTENT_SIZE);

    DIR *dir;
    if ((dir = opendir(filename)) != NULL) {
        printf("Parsing input from directory: %s\n", filename);
        struct dirent *dirent;
        while ((dirent = readdir(dir)) != NULL) {
            char *name = dirent->d_name;
            if (strcmp(name, ".") == 0) continue;
            if (strcmp(name, "..") == 0) continue;
            char path[PATH_MAX + 1];
            strcpy(path, filename);
            if (filename[strlen(filename) - 1] != '/') strcat(path, "/");
            strcat(path, name);
            add_input_file(path);
        }
    } else {
        printf("Parsing input from single file: %s\n", filename);
        add_input_file(filename);
    }
    closedir(dir);

    parse_input_content();

    // Input data is parsed. Start the threads

    int mkdir_err = mkdir(output, 0755);
    if (mkdir_err != 0 && errno != 17) print_error_line_q("Error %d (%s) while trying to make dir: %s", errno, strerror(errno), output); // errno 17 = file exists

    if (pthread_mutex_init(&mutex_urls, NULL) != 0) {
        print_error_line_q("Error %d (%s) while trying to create url mutex", errno, strerror(errno));
    }

    download_threads = calloc(n_threads, sizeof(download_thread_t));
    for (i = 0; i < n_threads; i++) {
        printf("Spawning thread #%0*d\n",
               n_threads == 1 ? 1 : (int) log10(n_threads - 1) + 1, // Fill with the correct amount of zeros
               i
        );

        download_thread_t download_thread = {
                        i,
                        0,
                        "",
                        NULL,
                        0,
                        0
        };

        // Thread creation
        int error = pthread_create(&download_thread.pthread,
                                   NULL,
                                   download_thread_f,
                                   (void *) (download_threads + i)
        );
        if (error != 0) print_error_line_q("Error %d (%s) while trying to create pthread with id = %d", errno, strerror(errno), i);

        download_threads[i] = download_thread;
    }

    for (i = 0; i < n_threads; i++) {
        pthread_join(download_threads[i].pthread, NULL);
    }
    printf("All threads returned!\n");
}

void cleanup() {
    printf("Cleaning up...\n");
    if (curl_initialized) curl_global_cleanup();
    if (download_threads != NULL) free(download_threads);
    if (input_content != NULL) free(input_content);
    if (url_list != NULL) free(url_list);
    exit(EXIT_SUCCESS);
}

int main(int argc, char **argv) {
    signal(SIGTERM, cleanup);
    signal(SIGINT, cleanup);

    initialize(argc, argv);

    cleanup();
    return EXIT_SUCCESS;
}
