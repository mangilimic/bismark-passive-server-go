package bismarkpassive

//import (
//    "bytes"
//    "io"
//    "io/ioutil"
//)

func parseSections(lines [][]byte) ([][][]byte) {
    sections := [][][]byte{}
    currentSection := [][]byte{}
    for _, line := range(lines) {
        if len(line) == 0 {
            sections = append(sections, currentSection)
            currentSection = [][]byte{}
        } else {
            currentSection = append(currentSection, line)
        }
    }
    if len(currentSection) > 0 {
        sections = append(sections, currentSection)
    }
    return sections
}

//func ParseTrace(source io.Reader) {
//    contents, err := ioutil.ReadAll(source)
//    if err != nil {
//        return nil, err
//    }
//    lines := bytes.Split(contents, []byte{"\n"})
//    sections := parseSections(lines)
//}
