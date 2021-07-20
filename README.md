# EVENT-DELETER

S3 버킷에서 특정 datasource 로 부터 받은 데이터를 지울때 사용하는 스크립트 입니다.

### 실행 방법
S3 버킷 읽을수 있는 AWS 프로필 설정
```shell
export AWS_PROFILE="profilewithaccess"

또는 아래 커맨드 사용해서 새로운 프로필 생성

aws configure --profile="profilewithaccess"
```

S3 버킷 설정
- 버킷 내에서 읽을 KEY 설정 ** 지금은 하드 코딩 되어 있음

### 로직

1. S3 버킷을/ KEY 를 사용해 recursive  하게 파일을 읽기
2. READ 한 파일 중에 제외하고 싶은 datasource 는 별도 delete-log 로 쓰기
3. 그 외 datasource events 는 새로운 이벤트 파일 (.gz) 파일 생성
4. 새롭게 생성된 파일을 기존 파일 위치로 덮어 쓰기

### OUTPUT
- 모든 파일을 읽으면서 진행 한 작업을 summary 한 master log file 한개와, 파일 별로 삭제한 이벤트를 담고 있는 event delete log 파일 여러개를 생성.
- 삭제한 이벤트가 없으면 delete log 파일이 생성 되지 않음





