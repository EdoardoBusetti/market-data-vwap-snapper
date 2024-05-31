from bintrees import FastAVLTree


class SimpleOrderBook:
    def __init__(self):
        # AVL trees are used as a main structure due its optimal performance features for this purpose

        self._asks = FastAVLTree()  # MIN Heap
        self._bids = FastAVLTree()  # MAX Heap

        self._total_ask_size = 0  # For monitoring purpose
        self._total_bid_size = 0  # For monitoring purpose

    def apply_update(self, side, size, price):
        if side == "ask":
            if size == 0:  # 0 size means that level should be dropped
                self._total_ask_size -= self._asks[
                    price
                ]  # check if we want to fail in case of level not here or just add warning about it
                self._asks.remove(price)
            else:
                self._total_ask_size += size
                self._asks.insert(price, size)
        else:  # bid
            if size == 0:  # 0 size means that level should be dropped
                self._total_bid_size -= self._bids[
                    price
                ]  # check if we want to fail in case of level not here or just add warning about it
                self._bids.remove(price)
            else:
                self._total_bid_size += size
                self._bids.insert(price, size)

    @property
    def total_ask_size(self):
        return self._total_ask_size

    @property
    def total_bid_size(self):
        return self._total_bid_size
