package entity

type PendingItem struct {
	ID   string    // номер, присвоенный на стороне redis
	Item FlushItem // данные key&data
}
