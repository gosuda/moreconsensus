package epaxos

import "bytes"

type resourceEntry struct {
	start []byte
	end   []byte
	point bool
	lanes keyLanes
}

type intervalNode struct {
	resource     *resourceEntry
	left, right  *intervalNode
	height       int8
	maxEnd       []byte
	maxInclusive bool
}

func resourceCompare(start []byte, point bool, end []byte, other *resourceEntry) int {
	if cmp := bytes.Compare(start, other.start); cmp != 0 {
		return cmp
	}
	if point != other.point {
		if point {
			return -1
		}
		return 1
	}
	return bytes.Compare(end, other.end)
}

func intervalHeight(n *intervalNode) int8 {
	if n == nil {
		return 0
	}
	return n.height
}

func upperGreater(end []byte, inclusive bool, other []byte, otherInclusive bool) bool {
	if cmp := bytes.Compare(end, other); cmp != 0 {
		return cmp > 0
	}
	return inclusive && !otherInclusive
}

func refreshInterval(n *intervalNode) {
	n.height = 1 + max(intervalHeight(n.left), intervalHeight(n.right))
	n.maxEnd = n.resource.end
	n.maxInclusive = n.resource.point
	if n.left != nil && upperGreater(n.left.maxEnd, n.left.maxInclusive, n.maxEnd, n.maxInclusive) {
		n.maxEnd, n.maxInclusive = n.left.maxEnd, n.left.maxInclusive
	}
	if n.right != nil && upperGreater(n.right.maxEnd, n.right.maxInclusive, n.maxEnd, n.maxInclusive) {
		n.maxEnd, n.maxInclusive = n.right.maxEnd, n.right.maxInclusive
	}
}

func rotateIntervalLeft(n *intervalNode) *intervalNode {
	right := n.right
	n.right = right.left
	right.left = n
	refreshInterval(n)
	refreshInterval(right)
	return right
}

func rotateIntervalRight(n *intervalNode) *intervalNode {
	left := n.left
	n.left = left.right
	left.right = n
	refreshInterval(n)
	refreshInterval(left)
	return left
}

func balanceInterval(n *intervalNode) *intervalNode {
	refreshInterval(n)
	balance := intervalHeight(n.left) - intervalHeight(n.right)
	if balance > 1 {
		if intervalHeight(n.left.left) < intervalHeight(n.left.right) {
			n.left = rotateIntervalLeft(n.left)
		}
		return rotateIntervalRight(n)
	}
	if balance < -1 {
		if intervalHeight(n.right.right) < intervalHeight(n.right.left) {
			n.right = rotateIntervalRight(n.right)
		}
		return rotateIntervalLeft(n)
	}
	return n
}

func findInterval(root *intervalNode, start []byte, point bool, end []byte) *resourceEntry {
	for root != nil {
		cmp := resourceCompare(start, point, end, root.resource)
		if cmp == 0 {
			return root.resource
		}
		if cmp < 0 {
			root = root.left
		} else {
			root = root.right
		}
	}
	return nil
}

func insertInterval(root *intervalNode, resource *resourceEntry) *intervalNode {
	if root == nil {
		return &intervalNode{resource: resource, height: 1, maxEnd: resource.end, maxInclusive: resource.point}
	}
	if resourceCompare(resource.start, resource.point, resource.end, root.resource) < 0 {
		root.left = insertInterval(root.left, resource)
	} else {
		root.right = insertInterval(root.right, resource)
	}
	return balanceInterval(root)
}

func minInterval(root *intervalNode) *intervalNode {
	for root.left != nil {
		root = root.left
	}
	return root
}

func deleteInterval(root *intervalNode, resource *resourceEntry) *intervalNode {
	if root == nil {
		return nil
	}
	cmp := resourceCompare(resource.start, resource.point, resource.end, root.resource)
	switch {
	case cmp < 0:
		root.left = deleteInterval(root.left, resource)
	case cmp > 0:
		root.right = deleteInterval(root.right, resource)
	case root.left == nil:
		return root.right
	case root.right == nil:
		return root.left
	default:
		successor := minInterval(root.right)
		root.resource = successor.resource
		root.right = deleteInterval(root.right, successor.resource)
	}
	return balanceInterval(root)
}

func intervalMayReachStart(n *intervalNode, start []byte) bool {
	if n == nil {
		return false
	}
	cmp := bytes.Compare(n.maxEnd, start)
	return cmp > 0 || cmp == 0 && n.maxInclusive
}

func queryIntervalOverlap(root *intervalNode, start, end []byte, yield func(*resourceEntry) bool) bool {
	if root == nil {
		return true
	}
	if intervalMayReachStart(root.left, start) && !queryIntervalOverlap(root.left, start, end, yield) {
		return false
	}
	resource := root.resource
	var overlaps bool
	if resource.point {
		overlaps = bytes.Compare(start, resource.start) <= 0 && bytes.Compare(resource.start, end) < 0
	} else {
		overlaps = bytes.Compare(resource.start, end) < 0 && bytes.Compare(start, resource.end) < 0
	}
	if overlaps && !yield(resource) {
		return false
	}
	if bytes.Compare(resource.start, end) < 0 {
		return queryIntervalOverlap(root.right, start, end, yield)
	}
	return true
}

func queryContainingSpans(root *intervalNode, point []byte, yield func(*resourceEntry) bool) bool {
	if root == nil {
		return true
	}
	if intervalMayReachStart(root.left, point) && !queryContainingSpans(root.left, point, yield) {
		return false
	}
	resource := root.resource
	if !resource.point && bytes.Compare(resource.start, point) <= 0 && bytes.Compare(point, resource.end) < 0 {
		if !yield(resource) {
			return false
		}
	}
	if bytes.Compare(resource.start, point) <= 0 {
		return queryContainingSpans(root.right, point, yield)
	}
	return true
}

func resourceEntryEmpty(resource *resourceEntry) bool {
	for _, lane := range resource.lanes {
		if lane.postings.root != nil || lane.retiredFloor != 0 {
			return false
		}
	}
	return true
}
