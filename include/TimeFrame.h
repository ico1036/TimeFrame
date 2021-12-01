#pragma once

#include "TimeNS.h"

#include "TTreeReader.h"
#include "TChain.h"
#include "TFile.h"

#include <random>

#include <functional>
#include <list>
#include <memory>
#include <chrono>
#include <iterator>
#include <algorithm>


template<class IDType, class TimeType, class RowType>
struct Templated_IDTimeRow
{
   Templated_IDTimeRow(IDType i, TimeType t, RowType r)
   : id(i), time(t), row(r)
   {

   }

   IDType id;
   TimeType time;
   RowType row;
};

template<class TimeType, class RowType>
struct Templated_IndexTimeRow
{
   Templated_IndexTimeRow(long i, TimeType t, RowType r)
   : index(i), time(t), row(r)
   {

   }

   long index;
   TimeType time;
   RowType row;
};

template<class RowType, class StateType>
struct Templated_RowState
{
   Templated_RowState(RowType r, StateType s)
   : row(r), state(s)
   {

   }

   RowType row;
   StateType state;
};

template<class TimeType, class RowType, class StateType>
struct Templated_TimeRowState
{
   Templated_TimeRowState(TimeType t, RowType r, StateType s)
   : time(t), row(r), state(s)
   {

   }

   TimeType time;
   RowType row;
   StateType state;
};

// Wrapper class arround TTree to keep track which tree has next piece of data chronologically
template<class RowReaderType, class IdBranchType, class TimeBranchType>
struct TimeFrameTree
{
   TimeFrameTree(TTree* pTree, std::string idBranchName, std::string timeBranchName)
   :  tree(pTree),
      reader(tree),
      id(reader, idBranchName.c_str()),
      time(reader, timeBranchName.c_str()),
      rowReader(reader)
   {
      lastTime = 0;
   }

   void prepareFirst(const std::function<bool(IdBranchType)>& idFilter, const std::function<void()>& preFilterCallback)
   {
      prepareNext(idFilter, preFilterCallback);
   }

   void prepareNext(const std::function<bool(IdBranchType)>& idFilter, const std::function<void()>& preFilterCallback)
   {
      hasNewRow = reader.Next();

      while(hasNewRow)
      {
         preFilterCallback();

         if(reader.IsChain())
         {
            TChain* chain = (TChain*) tree;
            if(numberTreesCounted <= chain->GetTreeNumber())
            {
               numberTreesCounted++;
               numberEntriesCounter += chain->GetTree()->GetEntries();
            }
         }

         if(idFilter(*id))
         {
            if(*time < lastTime)
            {
               messagesSkipped++;
               hasNewRow = reader.Next();
            }
            else
            {
               lastTime = *time;
               break;
            }
         }
         else
         {
            hasNewRow = reader.Next();
         }
      }
   }

   long getNumberEntries()
   {
      if(reader.IsChain())
      {
         TChain* chain = (TChain*) tree;
         return ((double) numberEntriesCounter) * chain->GetNtrees() / numberTreesCounted;
      }
      else
      {
         if(numberTreesCounted == 1)
         {
            return numberEntriesCounter;
         }
         else
         {
            numberTreesCounted++;
            numberEntriesCounter = reader.GetEntries(tree);
            //numberEntriesCounter = reader.GetEntries();
            return numberEntriesCounter;
         }
         
      }     
   }

   TTree* tree;

   TTreeReader reader;

   TTreeReaderValue<IdBranchType> id;
   TTreeReaderValue<TimeBranchType> time;

   RowReaderType rowReader;

   bool hasNewRow = true;

   TimeBranchType lastTime;

   int numberTreesCounted = 0;
   long numberEntriesCounter = 0;

   long messagesSkipped = 0;
};

template<class TimeType, class ItType, class getTimeType, class callbackType>
void resample(TimeType rate, TimeType from, TimeType to, ItType first, ItType last, getTimeType getTime, callbackType callback)
{
   if(from < getTime(first))
   {
      throw std::runtime_error("Resample error: first available entry comes after the start time");
   }

   TimeType nextTime = from;
   ItType it = first;

   while(nextTime <= to)
   {
      while(std::next(it, 1) != last && getTime(std::next(it, 1)) <= nextTime) // Short circuit evaluation
      {
         it++;
      }
      
      callback(nextTime, it);

      nextTime += rate;
   }
}

template<class RowType, class RowReaderType, class StateType, class IdBranchType = int, class TimeBranchType = TimeNS>
class TimeFrame
{
public:
   using IDTimeRow = Templated_IDTimeRow<IdBranchType, TimeBranchType, RowType>;
   using RowState = Templated_RowState<RowType, StateType>;
   using TimeRowState = Templated_TimeRowState<TimeBranchType, RowType, StateType>;
   using IndexTimeRow = Templated_IndexTimeRow<TimeBranchType, RowType>;

   TimeFrame()
   {
      idFilter = [](IdBranchType id){return true;};
   }

   ~TimeFrame()
   {
      if(!hasRun)
      {
         std::cout << "\n NOTE: TimeFrame object generated but not ran.\n";
      }
   }

   void add(TTree* tree, std::string idBranchName = "id", std::string timeBranchName = "time")
   {
      trees.emplace_back(std::make_unique<TimeFrameTree<RowReaderType, IdBranchType, TimeBranchType>>(tree, idBranchName, timeBranchName));
   }
   void add(TFile* pFile, std::string treeName, std::string idBranchName = "id", std::string timeBranchName = "time")
   {
      TTree* tree = (TTree*) pFile->Get(treeName.c_str());
      trees.emplace_back(std::make_unique<TimeFrameTree<RowReaderType, IdBranchType, TimeBranchType>>(tree, idBranchName, timeBranchName));
   }

   void setProgressBar(bool b)
   {
      showProgess = b;
   }

   void setIdFilter(std::function<bool(IdBranchType)> func)
   {
      idFilter = func;
   }
   void setIdFilter(IdBranchType id)
   {
      setIdFilter([=](IdBranchType rowId){return rowId == id;});
   }
   void setIdFilter(std::set<IdBranchType> ids)
   {
      setIdFilter([=](IdBranchType rowId){return ids.count(rowId) > 0;});
   }

   void setRowGenerator(std::function<std::optional<IDTimeRow>(IdBranchType, TimeBranchType, const RowType&, 
      IdBranchType, TimeBranchType, const RowType&)> func)
   {
      checkForGeneratedRow = func;
   }

   void setTrigger(std::function<bool(IdBranchType, TimeBranchType, const RowType&)> func)
   {
      trigger = func;
   }
   void setTrigger(std::function<bool(IdBranchType, TimeBranchType, const RowType&, const StateType&)> func)
   {
      storeStates = true;
      triggerWithState = func;
   }
   void setTriggerCooldown(TimeBranchType t)
   {
      triggerCooldown = t;
   }
   long getTriggerCount()
   {
      return triggerCount;
   }

   void setForEachRow(std::function<void(IdBranchType, TimeBranchType, const RowType&)> func)
   {
      forEachRow = func;
   }
   void setForEachRow(std::function<void(IdBranchType, TimeBranchType, const RowType&, const StateType&)> func)
   {
      storeStates = true;
      forEachRowWithState = func;
   }
   void setForEachRow(std::function<void(IdBranchType, TimeBranchType, const RowType&, const std::map<IdBranchType, StateType>&)> func)
   {
      storeStates = true;
      forEachRowWithAllState = func;
   }

   void setFilter(std::function<bool(IdBranchType, TimeBranchType, const RowType&)> func)
   {
      filter = func;
   }
   void setFilter(std::function<bool(IdBranchType, TimeBranchType, const RowType&, const StateType&)> func)
   {
      storeStates = true;
      filterWithState = func;
   }

   template<class T>
   void setAction(int fromMessage, int tillMessage, T func)
   {
      this->fromMessage = fromMessage;
      this->tillMessage = tillMessage;
      fromBasedOnMessage = true;
      tillBasedOnMessage = true;
      setAction(func);
   }
   template<class T>
   void setAction(TimeNS from, TimeNS till, T func)
   {
      this->from = from;
      this->till = till;
      setAction(func);
   }
   template<class T>
   void setAction(int fromMessage, TimeNS till, T func)
   {
      this->fromMessage = fromMessage;
      this->till = till;
      fromBasedOnMessage = true;
      setAction(func);
   }
   template<class T>
   void setAction(TimeNS from, int tillMessage, T func)
   {
      this->from = from;
      this->tillMessage = tillMessage;
      tillBasedOnMessage = true;
      setAction(func);
   }
   void setActionResampled(TimeNS from, TimeNS till, TimeBranchType interval, std::function<void(IdBranchType, TimeBranchType, const RowType&, const StateType&,
      const std::list<TimeRowState>&)> func)
   {
      this->from = from;
      this->till = till;
      resampleInterval = interval;
      resampleAction = true;
      storeStates = true;
      actionWithState = func;
   }
   void setActionResampled(TimeNS from, TimeNS till, TimeBranchType interval, std::function<void(IdBranchType, TimeBranchType, const RowType&, const StateType&,
      const std::list<std::pair<TimeBranchType, std::map<IdBranchType, RowState>>>&)> func)
   {
      this->from = from;
      this->till = till;
      resampleInterval = interval;
      resampleAction = true;
      storeStates = true;
      actionWithAllState = func;
   }

   void setStateInitializer(std::function<StateType(IdBranchType)> func)
   {
      stateInitializer = func;
   }
   void setStateUpdater(std::function<void(IdBranchType, TimeBranchType, StateType&, const RowType&)> func)
   {
      stateUpdater = func;
   }
   void setForEachSnapshot(TimeNS w, std::function<void(IdBranchType, TimeBranchType, const StateType&)> func)
   {
      windowSize = w;
      forEachSnapshot = func;
   }
   void setForEachSnapshot(TimeNS w, std::function<void(TimeBranchType, const std::map<IdBranchType, StateType>&)> func)
   {
      windowSize = w;
      forEachSnapshotAllStates = func;
   }

   void run()
   {
      try
      {
         hasRun = true;

         auto preFilterCallback = [&](){
            entriesProcessed++;
            updateProgressBar(false);
         };

         for(auto& tree : trees)
         {
            tree->prepareFirst(idFilter, preFilterCallback);   
         }

         startTime = std::chrono::steady_clock::now();

         bool firstMessage = true;
         IdBranchType lastId;
         TimeBranchType lastTime;
         RowType lastRow;

         bool finished = false;
         while((!finished) && (!stopRequested))
         {
            TimeBranchType earliestNextTime = std::numeric_limits<TimeBranchType>::max();
            int earliestNextIndex = -1;

            // Select tree with the earliest next row
            for(int i=0;i<trees.size();i++)
            {
               if(trees[i]->hasNewRow)
               {
                  if(*(trees[i]->time) < earliestNextTime)
                  {
                     earliestNextTime = *(trees[i]->time);
                     earliestNextIndex = i;
                  }
               }
            }

            if(earliestNextIndex != -1)
            {
               IdBranchType id = *(trees[earliestNextIndex]->id);
               TimeBranchType time = *(trees[earliestNextIndex]->time);

               RowType row = trees[earliestNextIndex]->rowReader.get();

               bool isRowGenerated = false;
               if(!firstMessage && checkForGeneratedRow)
               {
                  std::optional<IDTimeRow> generatedRow = checkForGeneratedRow(lastId, lastTime, lastRow, id, time, row);

                  if(generatedRow)
                  {
                     isRowGenerated = true;

                     id = generatedRow->id;
                     time = generatedRow->time;
                     row = generatedRow->row;
                  }
               }

               // Check if ID already seen before, init if not
               checkForNewID(id);

               // Check if action needs to be performed before this row updates the state
               checkForAction(id, time);

               // Update the state with this row
               checkForStateUpdate(id, time, row);

               // Check for handlers of each row, independent of trigger - filter - action system
               if(forEachRow) forEachRow(id, time, row);
               if(forEachRowWithState) forEachRowWithState(id, time, row, currentStates.at(id));
               if(forEachRowWithAllState) forEachRowWithAllState(id, time, row, currentStates);

               // Check if this row is a filter or trigger
               checkForFilter(id, time, row);
               checkForTrigger(id, time, row);

               firstMessage = false;
               lastId = id;
               lastTime = time;
               lastRow = row;

               if(!isRowGenerated)
               {
                  trees[earliestNextIndex]->prepareNext(idFilter, preFilterCallback);
               }
            }
            else
            {
               finished = true;
            }
         }

         for(auto& t : triggerData)
         {
            checkForAction(t.first, 0, true);
         }

         updateProgressBar(true);
      }
      catch(std::out_of_range& error)
      {
         updateProgressBar(false);

         std::cout << "\n\n";

         std::cout << "Out of range error thrown in TimeFrame class while looping: " << error.what() << "\n";
         std::cout << "Looping is aborted, execution is incomplete.\n";
      }
      catch(std::runtime_error& error)
      {
         updateProgressBar(false);

         std::cout << "\n\n";

         std::cout << "Error thrown in TimeFrame class while looping: " << error.what() << "\n";
         std::cout << "Looping is aborted, execution is incomplete.\n";
      }
   }

   void setLogData(std::string& data)
   {
      logData = data;
   }

   std::map<IdBranchType, StateType> getFinalStates()
   {
      return currentStates;
   }

   void requestStop()
   {
      stopRequested = true;
   }

private:
   void setAction(std::function<void(IdBranchType, TimeBranchType, const RowType&, const std::list<std::pair<TimeBranchType, RowType>>&)> func)
   {
      action = func;
   }
   void setAction(std::function<void(IdBranchType, TimeBranchType, const RowType&, const StateType&,
      const std::list<TimeRowState>&)> func)
   {
      storeStates = true;
      actionWithState = func;
   }
   void setAction(std::function<void(IdBranchType, TimeBranchType, const RowType&, const StateType&,
      const std::list<std::pair<TimeBranchType, std::map<IdBranchType, RowState>>>&)> func)
   {
      if(fromBasedOnMessage || tillBasedOnMessage)
      {
         throw std::runtime_error("Not supported: all state action with message borders");
      }
      storeStates = true;
      actionWithAllState = func;
   }

   void checkForNewID(IdBranchType currentId)
   {
      if(actionCount.count(currentId) == 0)
      {
         actionCount[currentId] = 0;
         lastTrigger[currentId] = TimeBranchType();
         triggerData[currentId] = std::list<IndexTimeRow>();

         rowsIndex[currentId] = std::list<long>();
         if(action)
         {
            rows[currentId] = std::list<std::pair<TimeBranchType, RowType>>();
         }
         else if(actionWithState)
         {
            rowStates[currentId] = std::list<TimeRowState>();
         }
         else if(actionWithAllState)
         {
            // Adding a new ID desyncs the state, clean them
            rowAllStates.clear();
         }

         if(stateInitializer)
         {
            currentStates.emplace(currentId, stateInitializer(currentId));
         }
      }
   }

   void checkForAction(IdBranchType currentId, TimeBranchType currentTime, bool endOfTree = false)
   {
      if(triggerData[currentId].size() > 0)
      {
         while(triggerData[currentId].size() > 0)
         {
            if((!tillBasedOnMessage && triggerData[currentId].front().time + till < currentTime)
               || (tillBasedOnMessage && triggerData[currentId].front().index + tillMessage < actionCount[currentId])
               || endOfTree)
            {
               removeOutdatedActionData(currentId, triggerData[currentId].front().time, triggerData[currentId].front().index);

               callActionHandlers(currentId);

               triggerData[currentId].pop_front();
               if (triggerStates.size() > 0)
               {
                  triggerStates[currentId].pop_front();
               }
            }
            else
            {
               break;
            }
         }
      }
      else
      {
         if(rowsIndex[currentId].size() > 0)
         {
            removeOutdatedActionData(currentId, currentTime, rowsIndex[currentId].back() - 1);
         }
      }
   }

   void removeOutdatedActionData(IdBranchType currentId, TimeBranchType triggerTime, long triggerIndex)
   {
      // If the action is resampled, to create the state at the first timestep of the action,
      // the last row just before the window of the action need to remain
      // Hence this variable keepPreWindowRows

      int keepPreWindowRows = resampleAction ? 1 : 0;

      if(action)
      {
         while(rows[currentId].size() > keepPreWindowRows)
         {
            if((!fromBasedOnMessage && std::next(rows[currentId].begin(), keepPreWindowRows)->first - from < triggerTime)
               || (fromBasedOnMessage && *std::next(rowsIndex[currentId].begin(), keepPreWindowRows) - fromMessage < triggerIndex))
            {
               rows[currentId].pop_front();
               rowsIndex[currentId].pop_front();
            }
            else
            {
               break;
            }
         }
      }
      else if(actionWithState)
      {
         while(rowStates[currentId].size() > keepPreWindowRows)
         {
            if((!fromBasedOnMessage && std::next(rowStates[currentId].begin(), keepPreWindowRows)->time - from < triggerTime)
               || (fromBasedOnMessage && *std::next(rowsIndex[currentId].begin(), keepPreWindowRows) - fromMessage < triggerIndex))
            {
               rowStates[currentId].pop_front();
               rowsIndex[currentId].pop_front();
            }
            else
            {
               break;
            }
         }
      }
      else if(actionWithAllState)
      {
         while(rowAllStates.size() > keepPreWindowRows)
         {
            if(std::next(rowAllStates.begin(), keepPreWindowRows)->first - from < triggerTime)
            {
               rowAllStates.pop_front();
            }
            else
            {
               break;
            }
         }
      }
   }

   void callActionHandlers(IdBranchType currentId)
   {
      // This will also trigger if there are 0 rows within the window
      if(action)
      {
         action(currentId,
            triggerData[currentId].front().time,
            triggerData[currentId].front().row,
            rows[currentId]);
      }
      else if(actionWithState)
      {
         if(!resampleAction)
         {
            actionWithState(currentId,
               triggerData[currentId].front().time,
               triggerData[currentId].front().row,
               triggerStates[currentId].front(),
               rowStates[currentId]);
         }
         else
         {
            if(rowStates[currentId].size() > 0)
            {
               auto i = rowStates[currentId].cbegin();
               TimeNS startTime = triggerData[currentId].front().time + from;

               if(i->time <= startTime)
               {
                  TimeNS lastAdded = startTime;

                  std::list<TimeRowState> rowsResampled;
                  rowsResampled.emplace_back(lastAdded, i->row, i->state);

                  while(std::next(i) != rowStates[currentId].cend())
                  {
                     while(lastAdded + resampleInterval < std::next(i)->time)
                     {
                        lastAdded = lastAdded + resampleInterval;
                        rowsResampled.emplace_back(lastAdded, i->row, i->state);
                     }

                     i++;
                  }

                  while(lastAdded + resampleInterval <= triggerData[currentId].front().time + till)
                  {
                     lastAdded = lastAdded + resampleInterval;
                     rowsResampled.emplace_back(lastAdded, i->row, i->state);
                  }

                  actionWithState(currentId,
                     triggerData[currentId].front().time,
                     triggerData[currentId].front().row,
                     triggerStates[currentId].front(),
                     rowsResampled);
               }
               else
               {
                  //throw std::runtime_error("Data missing for action handler");
                  std::cout << "Data missing for action handler, skip\n";
               }
            }
         }
      }
      else if(actionWithAllState)
      {
         if(!resampleAction)
         {
            actionWithAllState(currentId,
               triggerData[currentId].front().time,
               triggerData[currentId].front().row,
               triggerStates[currentId].front(),
               rowAllStates);
         }
         else
         {
            if(rowAllStates.size() > 0)
            {
               auto i = rowAllStates.cbegin();
               TimeNS startTime = triggerData[currentId].front().time + from;

               if(i->first <= startTime)
               {
                  TimeNS lastAdded = startTime;

                  std::list<std::pair<TimeBranchType, std::map<IdBranchType, RowState>>> rowsResampled;
                  rowsResampled.emplace_back(lastAdded, i->second);

                  while(std::next(i) != rowAllStates.cend() &&
                     lastAdded + resampleInterval <= triggerData[currentId].front().time + till)
                  {
                     while(lastAdded + resampleInterval < std::next(i)->first &&
                        lastAdded + resampleInterval <= triggerData[currentId].front().time + till)
                     {
                        lastAdded = lastAdded + resampleInterval;
                        rowsResampled.emplace_back(lastAdded, i->second);
                     }

                     i++;
                  }

                  while(lastAdded + resampleInterval <= triggerData[currentId].front().time + till)
                  {
                     lastAdded = lastAdded + resampleInterval;
                     rowsResampled.emplace_back(lastAdded, i->second);
                  }

                  actionWithAllState(currentId,
                     triggerData[currentId].front().time,
                     triggerData[currentId].front().row,
                     triggerStates[currentId].front(),
                     rowsResampled);
               }
               else
               {
                  //throw std::runtime_error("Data missing for action handler");
                  std::cout << "Data missing for action handler, skip\n";
               }
            }
         }
      }
   }

   void checkForStateUpdate(IdBranchType currentId, TimeBranchType currentTime, const RowType& row)
   {
      if(stateUpdater)
      {
         if(forEachSnapshot || forEachSnapshotAllStates)
         {
            if(lastWindow == 0)
            {
               lastWindow = (TimeNS)(currentTime / windowSize);
            }

            while(lastWindow < (TimeNS)(currentTime / windowSize))
            {
               lastWindow++;

               if(forEachSnapshot)
               {
                  for(const auto& [key, security] : currentStates)
                  {
                     forEachSnapshot(key, lastWindow * windowSize, security);
                  }
               }

               if(forEachSnapshotAllStates)
               {
                  forEachSnapshotAllStates(lastWindow * windowSize, currentStates);
               }
            }
         }

         stateUpdater(currentId, currentTime, currentStates.at(currentId), row);
      }
   }

   void checkForFilter(IdBranchType currentId, TimeBranchType currentTime, const RowType& row)
   {
      if(filterWithState)
      {
         if(filterWithState(currentId, currentTime, row, currentStates.at(currentId)))
         {
            storeRow(currentId, currentTime, row);
         }
      }
      else if(filter)
      {
         if(filter(currentId, currentTime, row))
         {
            storeRow(currentId, currentTime, row);
         }
      }
   }

   void storeRow(IdBranchType currentId, TimeBranchType currentTime, const RowType& row)
   {
      if(action)
      {
         rowsIndex[currentId].emplace_back(actionCount[currentId]);
         rows[currentId].emplace_back(currentTime, row);
         actionCount[currentId]++;
      }
      else if(actionWithState)
      {
         rowsIndex[currentId].emplace_back(actionCount[currentId]);
         rowStates[currentId].emplace_back(currentTime, row, currentStates.at(currentId));
         actionCount[currentId]++;
      }
      else if(actionWithAllState)
      {
         std::map<IdBranchType, RowState> rowAllState;

         for(auto c : currentStates)
         {
            rowAllState.emplace(c.first, RowState(row, c.second));
         }

         rowAllStates.emplace_back(currentTime, rowAllState);
         actionCount[currentId]++;
      }
   }

   void checkForTrigger(IdBranchType currentId, TimeBranchType currentTime, const RowType& row)
   {
      // actionCount[currentId] - 1 because triggers donts have unique indices, but are aligned with the last known action row.
      // and to avoid that if a single row is both an action and a trigger, it is not counted twice

      if(lastTrigger[currentId] + triggerCooldown <= currentTime)
      {
         //if((!fromBasedOnMessage && true) // TODO: add a fence to only allow triggers if suificient data is available
         //   || (fromBasedOnMessage && -fromMessage < actionCount[currentId] - 1))
         {
            if(triggerWithState)
            {
               if(triggerWithState(currentId, currentTime, row, currentStates.at(currentId)))
               {
                  lastTrigger[currentId] = currentTime;
                  triggerData[currentId].emplace_back(actionCount[currentId] - 1, currentTime, row);
                  triggerStates[currentId].emplace_back(currentStates.at(currentId));
                  triggerCount++;
               }
            }
            else if(trigger)
            {
               if(trigger(currentId, currentTime, row))
               {
                  lastTrigger[currentId] = currentTime;
                  triggerData[currentId].emplace_back(actionCount[currentId] - 1, currentTime, row);
                  if(actionWithState || actionWithAllState)
                  {
                     triggerStates[currentId].emplace_back(currentStates.at(currentId));
                  }
                  triggerCount++;
               }
            }
         }
      }
   }

   void updateProgressBar(bool finished)
   {
      if(showProgess)
      {
         if(nextProgressUpdate <= entriesProcessed)
         {
            nextProgressUpdate += 100000;
            
            long expectedNumberEntries = 0;
            for(auto& tree : trees)
            {
               expectedNumberEntries += tree->getNumberEntries();
            }

            double ratio = (double) entriesProcessed / (double) expectedNumberEntries;

            std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
            auto timeRunning = std::chrono::duration_cast<std::chrono::milliseconds>(now - startTime).count();
            auto timeRemaining = (timeRunning / ratio) * (1 - ratio);

            long rowsInMemory = 0;
            for(auto& ri : rowsIndex)
            {
               rowsInMemory += ri.second.size();
            }
            rowsInMemory += rowAllStates.size();

            std::cout << " Progress: ";
            std::cout << std::setfill(' ') << std::setw(6) << std::setprecision(1) << std::fixed << ratio * 100 << "% | ";
            std::cout << std::setfill(' ') << std::setw(6) << std::setprecision(0) << std::fixed << timeRunning / 1000.0 << " sec running |";
            std::cout << std::setfill(' ') << std::setw(6) << std::setprecision(0) << std::fixed << timeRemaining / 1000.0 << " sec remaining |";
            std::cout << std::setfill(' ') << std::setw(6) << std::setprecision(0) << std::fixed << rowsInMemory << " rows in memory |";
            std::cout << " " << logData; 
            std::cout << "               \r" << std::flush;
         }
         if(finished)
         {
            std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
            auto timeRunning = std::chrono::duration_cast<std::chrono::milliseconds>(now - startTime).count();

            std::cout << " Progress:  100.0% |";
            std::cout << std::setfill(' ') << std::setw(6) << std::setprecision(0) << std::fixed << timeRunning / 1000.0 << " sec running";
            std::cout << " (" << entriesProcessed << " rows)";
            std::cout << " " << logData; 
            std::cout << "                     \n" << std::flush;

            for(int i=0;i<trees.size();i++)
            {
               if(trees[i]->messagesSkipped > 0)
               {
                  std::cout << "Chain " << i << " has skipped " << trees[i]->messagesSkipped << " messages because they are out of order";
               }
            }
         }
      }
   }

private:
   bool storeStates = false;
   bool hasRun = true;
   bool stopRequested = false;

   std::vector<std::unique_ptr<TimeFrameTree<RowReaderType, IdBranchType, TimeBranchType>>> trees;

   std::function<bool(IdBranchType)> idFilter;

   // === ROW GENERATORS ===

   std::function<std::optional<IDTimeRow>(IdBranchType, TimeBranchType, const RowType&, 
      IdBranchType, TimeBranchType, const RowType&)> checkForGeneratedRow;

   // --- ROW GENERATORS ---

   // === TRIGGERS ===

   // Trigger handlers

   std::function<bool(IdBranchType, TimeBranchType, const RowType&)> trigger;
   std::function<bool(IdBranchType, TimeBranchType, const RowType&, const StateType&)> triggerWithState;

   // Trigger data

   std::map<IdBranchType, TimeBranchType> lastTrigger;
   std::map<IdBranchType, std::list<IndexTimeRow>> triggerData;
   std::map<IdBranchType, std::list<StateType>> triggerStates; // Optional use

   long triggerCount = 0;

   // Trigger config

   TimeBranchType triggerCooldown = 0;

   // --- TRIGGERS ---

   // === FILTERS ===

   std::function<bool(IdBranchType, TimeBranchType, const RowType&)> filter;
   std::function<bool(IdBranchType, TimeBranchType, const RowType&, const StateType&)> filterWithState;

   // --- FILTERS ---

   // === ACTIONS ===

   // Action handlers

   std::function<void(IdBranchType, TimeBranchType, const RowType&, const std::list<std::pair<TimeBranchType, RowType>>&)> action;
   std::function<void(IdBranchType, TimeBranchType, const RowType&, const StateType&, const std::list<TimeRowState>&)> actionWithState;
   std::function<void(IdBranchType, TimeBranchType, const RowType&, const StateType&,
      const std::list<std::pair<TimeBranchType, std::map<IdBranchType, RowState>>>&)> actionWithAllState;

   // Action data

   std::map<IdBranchType, long> actionCount;
   std::map<IdBranchType, std::list<long>> rowsIndex;

   // One of 3 is used
   std::map<IdBranchType, std::list<std::pair<TimeBranchType, RowType>>> rows;
   std::map<IdBranchType, std::list<TimeRowState>> rowStates;
   std::list<std::pair<TimeBranchType, std::map<IdBranchType, RowState>>> rowAllStates;

   // Action config

   TimeBranchType from = 0;
   TimeBranchType till = 0;
   int fromMessage;
   int tillMessage;
   bool fromBasedOnMessage = false;
   bool tillBasedOnMessage = false;
   TimeBranchType resampleInterval;
   bool resampleAction = false;

   // --- ACTIONS ---

   // === STATE AND ROW HANDLERS ===

   std::function<StateType(IdBranchType)> stateInitializer;
   std::function<void(IdBranchType, TimeBranchType, StateType&, const RowType&)> stateUpdater;

   std::function<void(IdBranchType, TimeBranchType, const StateType&)> forEachSnapshot;
   std::function<void(TimeBranchType, const std::map<IdBranchType, StateType>&)> forEachSnapshotAllStates;

   std::function<void(IdBranchType, TimeBranchType, const RowType&)> forEachRow;
   std::function<void(IdBranchType, TimeBranchType, const RowType&, const StateType&)> forEachRowWithState;
   std::function<void(IdBranchType, TimeBranchType, const RowType&, const std::map<IdBranchType, StateType>&)> forEachRowWithAllState;

   // data

   std::map<IdBranchType, StateType> currentStates;
   TimeNS lastWindow = 0;

   // config

   TimeNS windowSize = 0;

   // --- STATE AND ROW HANDLERS ---

   // === PROGRESS BAR ===

   long long entriesProcessed = 0;
   std::chrono::steady_clock::time_point startTime;
   bool showProgess = true;
   long nextProgressUpdate = 0;
   std::string logData;

   // --- PROGRESS BAR ---
};
