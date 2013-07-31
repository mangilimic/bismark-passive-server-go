package passive

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"github.com/sburnett/transformer/store"
)

func RecordPrinterPipeline(store store.Reader, keyFormat, valueFormat string) []transformer.PipelineStage {
	printer, err := newRecordPrinter(keyFormat, valueFormat)
	if err != nil {
		panic(err)
	}
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "Print",
			Reader:      store,
			Transformer: printer,
		},
	}
}

type recordPrinter struct {
	keys, values               []interface{}
	keysIgnored, valuesIgnored []bool
	keysRaw, valuesRaw         bool
}

func (sessionKey SessionKey) String() string {
	return fmt.Sprintf("%s,%s,%d", sessionKey.NodeId, sessionKey.AnonymizationContext, sessionKey.SessionId)
}

func parsePrintFormatString(format string) ([]interface{}, []bool, bool, error) {
	var values []interface{}
	var ignored []bool
	var raw bool
	for _, format := range strings.Split(format, ",") {
		if format[0] == '-' {
			ignored = append(ignored, true)
			format = format[1:]
		} else {
			ignored = append(ignored, false)
		}
		switch format {
		case "[]byte":
			values = append(values, new([]byte))
		case "[][]byte":
			values = append(values, new([][]byte))
		case "string":
			values = append(values, new(string))
		case "[]string":
			values = append(values, new([]string))
		case "bool":
			values = append(values, new(bool))
		case "int8":
			values = append(values, new(int8))
		case "[]int8":
			values = append(values, new([]int8))
		case "uint8":
			values = append(values, new(uint8))
		case "[]uint8":
			values = append(values, new([]uint8))
		case "int16":
			values = append(values, new(int16))
		case "[]int16":
			values = append(values, new([]int16))
		case "uint32":
			values = append(values, new(uint32))
		case "[]uint32":
			values = append(values, new([]uint32))
		case "int32":
			values = append(values, new(int32))
		case "[]int32":
			values = append(values, new([]int32))
		case "uint64":
			values = append(values, new(uint64))
		case "[]uint64":
			values = append(values, new([]uint64))
		case "int64":
			values = append(values, new(int64))
		case "[]int64":
			values = append(values, new([]int64))
		case "SessionKey":
			values = append(values, new(SessionKey))
		case "raw":
			raw = true
		default:
			return nil, nil, false, fmt.Errorf("Invalid format specifier: %v", format)
		}
	}
	return values, ignored, raw, nil
}

func newRecordPrinter(keyFormat, valueFormat string) (*recordPrinter, error) {
	var printer recordPrinter
	if len(keyFormat) > 0 {
		if keys, ignored, raw, err := parsePrintFormatString(keyFormat); err != nil {
			return nil, err
		} else {
			printer.keys = keys
			printer.keysIgnored = ignored
			printer.keysRaw = raw
		}
	}
	if len(valueFormat) > 0 {
		if values, ignored, raw, err := parsePrintFormatString(valueFormat); err != nil {
			return nil, err
		} else {
			printer.values = values
			printer.valuesIgnored = ignored
			printer.valuesRaw = raw
		}
	}
	return &printer, nil
}

func (printer *recordPrinter) Do(inputChan, outputChan chan *store.Record) {
	for record := range inputChan {
		if printer.keys != nil {
			remainder := key.DecodeOrDie(record.Key, printer.keys...)
			printed := 0
			for idx, k := range printer.keys {
				if printer.keysIgnored[idx] {
					continue
				}
				v := reflect.ValueOf(k)
				if printed > 0 {
					fmt.Print(",")
				}
				fmt.Print(v.Elem().Interface())
				printed++
			}
			if len(remainder) > 0 && printer.keysRaw {
				fmt.Printf(",%v", remainder)
			}
			fmt.Printf(": ")
		} else if printer.keysRaw {
			fmt.Printf("%v: ", record.Key)
		}
		if printer.values != nil {
			remainder := key.DecodeOrDie(record.Value, printer.values...)
			printed := 0
			for idx, value := range printer.values {
				if printer.valuesIgnored[idx] {
					continue
				}
				v := reflect.ValueOf(value)
				if printed > 0 {
					fmt.Print(",")
				}
				fmt.Print(v.Elem().Interface())
				printed++
			}
			if len(remainder) > 0 && printer.valuesRaw {
				fmt.Printf(",%v", remainder)
			}
		} else if printer.valuesRaw {
			fmt.Printf("%v", record.Value)
		}
		fmt.Printf("\n")
	}
}
