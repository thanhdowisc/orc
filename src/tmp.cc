#include <iostream>
#include "RLE.hh"

int main(int, char*[]) {
  unsigned char buffer[] = {0x61, 0xff, 0x64, 0xfb, 0x02, 0x03, 0x4, 0x7, 0xb};
  std::unique_ptr<orc::SeekableInputStream> stream =
    std::unique_ptr<orc::SeekableInputStream>
      (new orc::SeekableArrayInputStream(buffer, 0, 9));
  std::unique_ptr<orc::RleDecoder> rle = 
    orc::createRleDecoder(std::move(stream), false, orc::VERSION_1);
  orc::LongVectorBatch myBatch(1024);
  rle->next(myBatch, 105);
  for(int i=0; i < 105; ++i) {
    std::cout << "Value: " << i << " = " << myBatch.data[i] << "\n"; 
  }
}
