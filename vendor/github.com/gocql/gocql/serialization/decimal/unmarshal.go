package decimal

import (
	"fmt"
	"gopkg.in/inf.v0"
	"reflect"
)

func Unmarshal(data []byte, value interface{}) error {
	switch v := value.(type) {
	case nil:
		return nil
	case *inf.Dec:
		return DecInfDec(data, v)
	case **inf.Dec:
		return DecInfDecR(data, v)
	case *string:
		return DecString(data, v)
	case **string:
		return DecStringR(data, v)
	default:
		// Custom types (type MyString string) can be deserialized only via `reflect` package.
		// Later, when generic-based serialization is introduced we can do that via generics.
		rv := reflect.ValueOf(value)
		rt := rv.Type()
		if rt.Kind() != reflect.Ptr {
			return fmt.Errorf("failed to unmarshal decimal: unsupported value type (%T)(%#[1]v)", value)
		}
		if rt.Elem().Kind() != reflect.Ptr {
			return DecReflect(data, rv)
		}
		return DecReflectR(data, rv)
	}
}
