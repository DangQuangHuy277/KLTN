package userinfo

type ServiceImpl struct {
	repo Repository
}

func NewServiceImpl(repo Repository) *ServiceImpl {
	return &ServiceImpl{
		repo: repo,
	}
}

//func (s *ServiceImpl) GetUserInfo(req GetUserInfoRequest) (*GetUserInfoResponse, error) {
//	userInfo, err := s.repo.GetUserInfo(req)
//	if err != nil {
//		return nil, err
//	}
//	return userInfo, nil
//}
