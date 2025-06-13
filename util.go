package main

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"strconv"
	"unsafe"
)

func FnvHash(s string) (uint32, error) {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	if err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

func EstimateSize(v interface{}) uintptr {
	seen := make(map[uintptr]bool)
	return estimate(reflect.ValueOf(v), seen)
}

func estimate(val reflect.Value, seen map[uintptr]bool) uintptr {
	if !val.IsValid() {
		return 0
	}

	switch val.Kind() {
	case reflect.Ptr:
		if val.IsNil() {
			return unsafe.Sizeof(val.Pointer())
		}
		ptr := val.Pointer()
		if seen[ptr] {
			return unsafe.Sizeof(ptr)
		}
		seen[ptr] = true
		return unsafe.Sizeof(ptr) + estimate(val.Elem(), seen)

	case reflect.Struct:
		var size uintptr
		for i := 0; i < val.NumField(); i++ {
			size += estimate(val.Field(i), seen)
		}
		return size

	case reflect.Slice:
		if val.IsNil() {
			return unsafe.Sizeof(val.Pointer()) + unsafe.Sizeof(val.Len())
		}
		var size = unsafe.Sizeof(val.Pointer()) + unsafe.Sizeof(val.Len())
		for i := 0; i < val.Len(); i++ {
			size += estimate(val.Index(i), seen)
		}
		return size

	case reflect.String:
		return unsafe.Sizeof("") + uintptr(len(val.String()))

	case reflect.Map:
		if val.IsNil() {
			return unsafe.Sizeof(val.Pointer())
		}
		var size = unsafe.Sizeof(val.Pointer())
		for _, key := range val.MapKeys() {
			size += estimate(key, seen)
			size += estimate(val.MapIndex(key), seen)
		}
		return size

	default:
		return val.Type().Size()
	}
}

func AutoToInt(a interface{}) (n int) {
	if a == nil {
		return 0
	}
	switch s := a.(type) {
	case string:
		n, _ = strconv.Atoi(s)
	case *string:
		n, _ = strconv.Atoi(*s)
	case bool:
		if s {
			n = 1
		} else {
			n = 0
		}
	case *bool:
		if *s {
			n = 1
		} else {
			n = 0
		}
	case int:
		n = s
	case *int:
		n = *s
	case int8:
		n = int(s)
	case *int8:
		n = int(*s)
	case int16:
		n = int(s)
	case *int16:
		n = int(*s)
	case int32:
		n = int(s)
	case *int32:
		n = int(*s)
	case int64:
		n = int(s)
	case *int64:
		n = int(*s)
	case float32:
		n = int(s)
	case *float32:
		n = int(*s)
	case float64:
		n = int(s)
	case *float64:
		n = int(*s)
	case uint:
		n = int(s)
	case *uint:
		n = int(*s)
	case uint8:
		n = int(s)
	case *uint8:
		n = int(*s)
	case uint16:
		n = int(s)
	case *uint16:
		n = int(*s)
	case uint32:
		n = int(s)
	case *uint32:
		n = int(*s)
	case uint64:
		n = int(s)
	case *uint64:
		n = int(*s)
	default:
		n = 0
	}
	return
}

func AutoToString(a interface{}) string {
	if a == nil {
		return ""
	}
	switch s := a.(type) {
	case error:
		e, ok := a.(error)
		if ok && e != nil {
			return a.(error).Error()
		} else {
			return ""
		}
	case string:
		return s
	case *string:
		return *s
	case bool:
		return strconv.FormatBool(s) //fmt.Sprintf("%t", s)
	case *bool:
		return strconv.FormatBool(*s) //fmt.Sprintf("%t", *s)
	case int:
		return strconv.FormatInt(int64(s), 10)
	case *int:
		return strconv.FormatInt(int64(*s), 10)
	case int32:
		return strconv.FormatInt(int64(s), 10)
	case *int32:
		return strconv.FormatInt(int64(*s), 10) //fmt.Sprintf("%d", *s)
	case int64:
		return strconv.FormatInt(s, 10)
	case *int64:
		return strconv.FormatInt((*s), 10) //fmt.Sprintf("%d", *s)
	case float64:
		return fmt.Sprintf("%g", s)
	case *float64:
		return fmt.Sprintf("%g", *s)
	case uint:
		return strconv.FormatUint(uint64(s), 10)
	case *uint:
		return strconv.FormatUint(uint64(*s), 10)
	case uint32:
		return strconv.FormatUint(uint64(s), 10)
	case *uint32:
		return strconv.FormatUint(uint64(*s), 10)
	case uint64:
		return strconv.FormatUint(s, 10)
	case *uint64:
		return strconv.FormatUint((*s), 10)
	}

	return ""
}
