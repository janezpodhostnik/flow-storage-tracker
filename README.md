# Flow Storage Tracker

The Flow Storage Tracker is a executable that tracks storage used and the growth of storage used by accounts on the Flow blockchain.

## How does it work

It uses the Flow Batch Scan library to scan the entire flow chain for accounts and their storage usage. It then follows all the changes to the storage usage of the accounts.

The data is stored in a postress database.