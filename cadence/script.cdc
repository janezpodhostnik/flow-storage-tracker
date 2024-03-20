
pub struct AccountInfo {
    pub(set) var address: Address
    pub(set) var storageUsed: UInt64

    init(address: Address) {
        self.address = address
        self.storageUsed = 0
    }
}

pub fun main(addresses: [Address]): [AccountInfo] {
    let infos: [AccountInfo] = []
    for address in addresses {
        let account = getAccount(address)
        let info = AccountInfo(address: address)

        info.storageUsed = account.storageUsed
        infos.append(info)
    }
    return infos
}
