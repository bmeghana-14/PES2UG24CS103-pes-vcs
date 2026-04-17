// tree.c — Tree object serialization and construction

#include "tree.h"
#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── Mode Constants ─────────────────────────────────────────────────────────

#define MODE_FILE      0100644
#define MODE_EXEC      0100755
#define MODE_DIR       0040000

// ─── PROVIDED ───────────────────────────────────────────────────────────────

uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;

    if (S_ISDIR(st.st_mode))  return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];

        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1;

        char mode_str[16] = {0};
        size_t mode_len = (size_t)(space - ptr);
        if (mode_len >= sizeof(mode_str)) return -1;
        memcpy(mode_str, ptr, mode_len);
        entry->mode = (uint32_t)strtol(mode_str, NULL, 8);

        ptr = space + 1;

        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1;

        size_t name_len = (size_t)(null_byte - ptr);
        if (name_len >= sizeof(entry->name)) return -1;
        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0';

        ptr = null_byte + 1;

        if (ptr + HASH_SIZE > end) return -1;
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }
    return 0;
}

static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    size_t max_size = tree->count * 296 + 1;
    if (max_size == 0) max_size = 1;
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    Tree sorted_tree = *tree;
    qsort(sorted_tree.entries, (size_t)sorted_tree.count, sizeof(TreeEntry), compare_tree_entries);

    size_t offset = 0;
    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];

        int written = sprintf((char *)buffer + offset, "%o %s", entry->mode, entry->name);
        if (written < 0) {
            free(buffer);
            return -1;
        }
        offset += (size_t)written + 1;

        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;
    return 0;
}

// ─── tree_from_index ────────────────────────────────────────────────────────

static int cmp_index_path(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path, ((const IndexEntry *)b)->path);
}

static int first_path_component(const char *path, char *name, size_t name_cap, const char **rest_out) {
    const char *slash = strchr(path, '/');
    if (!slash) {
        if (strlen(path) >= name_cap) return -1;
        strcpy(name, path);
        *rest_out = "";
        return 0;
    }
    size_t len = (size_t)(slash - path);
    if (len >= name_cap) return -1;
    memcpy(name, path, len);
    name[len] = '\0';
    *rest_out = slash + 1;
    return 0;
}

static int write_tree_slice(IndexEntry *entries, int from, int to, ObjectID *out_id);

static int write_tree_slice(IndexEntry *entries, int from, int to, ObjectID *out_id) {
    if (from >= to) {
        Tree empty = {0};
        void *data;
        size_t len;
        if (tree_serialize(&empty, &data, &len) != 0) return -1;
        int rc = object_write(OBJ_TREE, data, len, out_id);
        free(data);
        return rc;
    }

    Tree tree;
    tree.count = 0;

    int i = from;
    while (i < to) {
        char comp[256];
        const char *rest_i;
        if (first_path_component(entries[i].path, comp, sizeof(comp), &rest_i) != 0) return -1;

        int j = i + 1;
        while (j < to) {
            char comp2[256];
            const char *rest_j;
            if (first_path_component(entries[j].path, comp2, sizeof(comp2), &rest_j) != 0) return -1;
            if (strcmp(comp, comp2) != 0) break;
            j++;
        }

        if (j == i + 1 && rest_i[0] == '\0') {
            if (tree.count >= MAX_TREE_ENTRIES) return -1;
            TreeEntry *e = &tree.entries[tree.count++];
            e->mode = entries[i].mode;
            e->hash = entries[i].hash;
            strncpy(e->name, comp, sizeof(e->name) - 1);
            e->name[sizeof(e->name) - 1] = '\0';
        } else {
            IndexEntry *sub = malloc((size_t)(j - i) * sizeof(IndexEntry));
            if (!sub) return -1;
            int sub_n = 0;
            for (int k = i; k < j; k++) {
                sub[sub_n] = entries[k];
                const char *p = entries[k].path;
                size_t clen = strlen(comp);
                if (p[clen] == '/') {
                    if (strlen(p + clen + 1) >= sizeof(sub[sub_n].path)) {
                        free(sub);
                        return -1;
                    }
                    strcpy(sub[sub_n].path, p + clen + 1);
                } else {
                    sub[sub_n].path[0] = '\0';
                }
                sub_n++;
            }

            ObjectID sub_id;
            int rc = write_tree_slice(sub, 0, sub_n, &sub_id);
            free(sub);
            if (rc != 0) return -1;

            if (tree.count >= MAX_TREE_ENTRIES) return -1;
            TreeEntry *e = &tree.entries[tree.count++];
            e->mode = MODE_DIR;
            e->hash = sub_id;
            strncpy(e->name, comp, sizeof(e->name) - 1);
            e->name[sizeof(e->name) - 1] = '\0';
        }

        i = j;
    }

    void *blob;
    size_t blen;
    if (tree_serialize(&tree, &blob, &blen) != 0) return -1;
    int rc = object_write(OBJ_TREE, blob, blen, out_id);
    free(blob);
    return rc;
}

int tree_from_index(ObjectID *id_out) {
    if (!id_out) return -1;

    Index idx;
    if (index_load(&idx) != 0) return -1;

    if (idx.count == 0) {
        Tree empty = {0};
        void *data;
        size_t len;
        if (tree_serialize(&empty, &data, &len) != 0) return -1;
        int rc = object_write(OBJ_TREE, data, len, id_out);
        free(data);
        return rc;
    }

    IndexEntry *sorted = malloc((size_t)idx.count * sizeof(IndexEntry));
    if (!sorted) return -1;
    memcpy(sorted, idx.entries, (size_t)idx.count * sizeof(IndexEntry));
    qsort(sorted, (size_t)idx.count, sizeof(IndexEntry), cmp_index_path);

    int rc = write_tree_slice(sorted, 0, idx.count, id_out);
    free(sorted);
    return rc;
}
