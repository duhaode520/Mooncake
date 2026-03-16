// mooncake-store/src/polling_oplog_change_notifier.cpp
// Placeholder - will be implemented in Task 8

#include "polling_oplog_change_notifier.h"

namespace mooncake {

PollingOpLogChangeNotifier::PollingOpLogChangeNotifier(OpLogStore* store,
                                                       int poll_interval_ms)
    : store_(store), poll_interval_ms_(poll_interval_ms) {}

PollingOpLogChangeNotifier::~PollingOpLogChangeNotifier() { Stop(); }

ErrorCode PollingOpLogChangeNotifier::Start(uint64_t /*start_sequence_id*/,
                                            EntryCallback /*on_entry*/,
                                            ErrorCallback /*on_error*/) {
    return ErrorCode::INTERNAL_ERROR;  // Not yet implemented
}

void PollingOpLogChangeNotifier::Stop() {}

bool PollingOpLogChangeNotifier::IsHealthy() const { return false; }

void PollingOpLogChangeNotifier::PollLoop() {}

}  // namespace mooncake
