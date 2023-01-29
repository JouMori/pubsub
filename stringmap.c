#include <stringmap.h>
#include <string.h>
#include <stdlib.h>

/* Struct representing a node in a singly linked list of StringMapItems */
typedef struct StringMapNode {
    StringMapItem data;
    struct StringMapNode* next;
} StringMapNode;

/* Struct containing the root node in the StringMap singly linked list */
struct StringMap {
    StringMapNode* root;
};

StringMap* stringmap_init(void) {
    StringMap* head = (StringMap*) malloc(sizeof(StringMap));
    head->root = NULL;
    return head;
}

void stringmap_free(StringMap* sm) {
    // Argument is null
    if (sm == NULL) {
	return;
    }

    StringMapNode* currentNode = sm->root;
    StringMapNode* prevFree = NULL;

    // Iterate through list
    while (currentNode != NULL) {
	prevFree = currentNode;
	currentNode = currentNode->next;
	free(prevFree->data.key);
	free(prevFree);
    }
    free(sm);
}

void* stringmap_search(StringMap* sm, char* key) {
    StringMapNode* currentNode = sm->root;

    // Iterate through list
    while (1) {

	// End of list reached
	if (currentNode == NULL) {
	    return NULL;
	}
	
	// Match found
	if (!strcmp(key, currentNode->data.key)) {
	    break;
	}
	currentNode = currentNode->next;
    }
    return currentNode->data.item;
}

int stringmap_add(StringMap* sm, char* key, void* item) {

    // Arguments null
    if (sm == NULL || key == NULL || item == NULL) {
	return 0;
    }

    // Finding end of list
    StringMapNode* currentNode = sm->root;
    StringMapNode* previousNode = NULL;
    while (currentNode != NULL) {

	// Node already exists in list
	if (!strcmp(key, currentNode->data.key)) {
	    return 0;
	}
	previousNode = currentNode;
	currentNode = currentNode->next;
    }

    // Allocating node to add
    StringMapNode* nextNode = (StringMapNode*) malloc(sizeof(StringMapNode));

    // Keep track of next node 
    if (previousNode != NULL) {
	previousNode->next = nextNode;
    }

    // Initialising node to add  with given key and item
    nextNode->next = NULL;
    nextNode->data.key = (char*) malloc(strlen(key) + 1);
    strcpy(nextNode->data.key, key);
    nextNode->data.item = item;

    // Set root to next node
    if (sm->root == NULL) {
	sm->root = nextNode;
    }
    return 1;
}

int stringmap_remove(StringMap* sm, char* key) {

    // Arguments null
    if (sm == NULL || key == NULL) {
	return 0;
    }

    // Iterate through list
    StringMapNode* currentNode = sm->root;
    StringMapNode* previousNode = NULL;
    while (1) {

	// Node not found 
	if (currentNode == NULL) {
	    return 0;
	}

	// Node found
	if (!strcmp(currentNode->data.key, key)) {
	    break;
	}

	previousNode = currentNode;
	currentNode = currentNode->next;
    }

    StringMapNode* nextNode = currentNode->next;
    
    // Keep track of next node
    if (previousNode != NULL) {
	previousNode->next = nextNode;
    }

    // Ensure that if last node is freed, the root is null
    if (previousNode == NULL && nextNode == NULL) {
	sm->root = NULL;
    } 
    
    // Set root to next node
    if (previousNode == NULL && nextNode != NULL) {
	sm->root = nextNode;
    }

    free(currentNode->data.key);
    free(currentNode);
    
    return 1;
}

StringMapItem* stringmap_iterate(StringMap* sm, StringMapItem* prev) {
    // Argument null
    if (sm == NULL) {
	return NULL;
    }

    // Return first entry
    if (prev == NULL) {
	return &(sm->root->data);
    }

    // Iterate
    StringMapNode* currentNode = sm->root;
    while (&(currentNode->data) != prev) {
	currentNode = currentNode->next;
	if (currentNode == NULL) {
	    return NULL;
	}
    }

    StringMapNode* targetNode = currentNode->next;

    // End of list reached
    if (targetNode == NULL) {
	return NULL;
    }

    return &(targetNode->data);
}
