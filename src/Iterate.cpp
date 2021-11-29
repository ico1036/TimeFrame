#include "../include/TimeFrame.h"
#include "../include/TChainFactory.h"

struct Message
{
    double x;
    double y;
    double z;
};

struct MessageReader
{
public:
   TTreeReaderValue<double> x;
   TTreeReaderValue<double> y;
   TTreeReaderValue<double> z;

   MessageReader(TTreeReader& reader)
   :  x(reader, "x"),
      y(reader, "y"),
      z(reader, "z")
   {

   }

   Message get()
   {
      Message message;

      message.x = *x;
      message.y = *y;
      message.z = *z;

      return message;
   }
};

class LimitOrderBook
{
public:
    void update(TimeNS time, const Message& message)
    {
        sumX += message.x;
        maxY = std::max(maxY, message.y);
        lastZ = message.z;
    }

    double sumX = 0;
    double maxY = 0;
    double lastZ = 0;
};

void Iterate()
{
    auto chain1 = makeChain("messages", "examples/", ".*1.*");
    auto chain2 = makeChain("messages", "examples/", ".*2.*");
    auto chain3 = makeChain("messages", "examples/", ".*3.*");

    TimeFrame<Message, MessageReader, LimitOrderBook> timeFrame;

    // Multiple chains are synchronized
    timeFrame.add(chain1);
    timeFrame.add(chain2);
    timeFrame.add(chain3);

    timeFrame.setStateInitializer([](int id)
    {
        return LimitOrderBook();
    });

    timeFrame.setStateUpdater([](int id, TimeNS time, LimitOrderBook& lob, const Message& message)
    {
        lob.update(time, message);
    });

    timeFrame.setForEachSnapshot(T_Hour, [&](TimeNS time, const map<int, LimitOrderBook>& lobs)
    {
        std::cout << lobs.size() << " internal states tracked at " << nsToTimestamp(time) << "                                             \n";
    });

    timeFrame.run();
}