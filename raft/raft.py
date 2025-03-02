import asyncio
import random
from concurrent import futures
import grpc
import raft_pb2
import raft_pb2_grpc

class RaftNode(raft_pb2_grpc.RaftServicer):
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers
        self.current_term = 0
        self.voted_for = None
        self.state = "follower"
        self.leader_id = None

    async def start_election(self):
        """Trigger an election when a node times out waiting for a leader."""
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        votes = 1

        for peer in self.peers:
            try:
                response = peer.RequestVote(raft_pb2.VoteRequest(
                    term=self.current_term,
                    candidate_id=self.node_id
                ))
                if response.vote_granted:
                    votes += 1
            except:
                continue

        if votes > len(self.peers) // 2:
            self.state = "leader"
            self.leader_id = self.node_id
            print(f"Node {self.node_id} is now the leader for term {self.current_term}")

    async def election_timeout(self):
        """Randomized election timeout to prevent split votes."""
        await asyncio.sleep(random.uniform(3, 5))
        if self.state != "leader":
            await self.start_election()

# Server setup
async def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServicer_to_server(RaftNode(node_id=1, peers=[]), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    await asyncio.sleep(60)  # Run server for 60 seconds

if __name__ == "__main__":
    asyncio.run(serve())
