package remoting

import "fmt"

func WrapResponse(msg interface{}) *RapidResponse {
	resp := &RapidResponse{}
	switch rmsg := msg.(type) {
	case *JoinResponse:
		resp.Content = &RapidResponse_JoinResponse{
			JoinResponse: rmsg,
		}
	case *ProbeResponse:
		resp.Content = &RapidResponse_ProbeResponse{
			ProbeResponse: rmsg,
		}
	case *ConsensusResponse:
		resp.Content = &RapidResponse_ConsensusResponse{
			ConsensusResponse: rmsg,
		}
	case *Response:
		resp.Content = &RapidResponse_Response{
			Response: rmsg,
		}
	default:
		panic(fmt.Sprintf("unsupported response type: %T", msg))
	}
	return resp
}

func WrapRequest(msg interface{}) *RapidRequest {
	req := &RapidRequest{}
	switch rmsg := msg.(type) {
	case *PreJoinMessage:
		req.Content = &RapidRequest_PreJoinMessage{
			PreJoinMessage: rmsg,
		}
	case *JoinMessage:
		req.Content = &RapidRequest_JoinMessage{
			JoinMessage: rmsg,
		}
	case *BatchedAlertMessage:
		req.Content = &RapidRequest_BatchedAlertMessage{
			BatchedAlertMessage: rmsg,
		}
	case *ProbeMessage:
		req.Content = &RapidRequest_ProbeMessage{
			ProbeMessage: rmsg,
		}
	case *FastRoundPhase2BMessage:
		req.Content = &RapidRequest_FastRoundPhase2BMessage{
			FastRoundPhase2BMessage: rmsg,
		}
	case *Phase1AMessage:
		req.Content = &RapidRequest_Phase1AMessage{
			Phase1AMessage: rmsg,
		}
	case *Phase1BMessage:
		req.Content = &RapidRequest_Phase1BMessage{
			Phase1BMessage: rmsg,
		}
	case *Phase2AMessage:
		req.Content = &RapidRequest_Phase2AMessage{
			Phase2AMessage: rmsg,
		}
	case *Phase2BMessage:
		req.Content = &RapidRequest_Phase2BMessage{
			Phase2BMessage: rmsg,
		}
	}
	return req
}
