// Copyright (C) 2017 ScyllaDB

package scheduler

import "github.com/derekparker/trie"

func findNode(node *trie.Node, runes []rune) *trie.Node {
	if node == nil {
		return nil
	}

	if len(runes) == 0 {
		return node
	}

	n, ok := node.Children()[runes[0]]
	if !ok {
		return nil
	}

	var nrunes []rune
	if len(runes) > 1 {
		nrunes = runes[1:]
	} else {
		nrunes = runes[0:0]
	}

	return findNode(n, nrunes)
}

func childNode(node *trie.Node, r rune) *trie.Node {
	if node == nil {
		return nil
	}
	return node.Children()[r]
}

func leafNode(node *trie.Node) *trie.Node {
	if node == nil {
		return nil
	}
	firstChild := func(node *trie.Node) *trie.Node {
		for _, c := range node.Children() {
			return c
		}
		return nil
	}
	for {
		switch len(node.Children()) {
		case 0:
			return node
		case 1:
			node = firstChild(node)
		default:
			return nil
		}
	}
}
