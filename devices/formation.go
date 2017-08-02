package devices

import "sync"

type stateMap map[string]interface{}

// deviceID -> device state
type deviceStateMap map[string]stateMap

type formationS struct {
	state   stateMap
	devices deviceStateMap
}

// FormationMap ...
type FormationMap struct {
	m map[string]*formationS
	l sync.RWMutex
}

// NewFormationMap ...
func NewFormationMap() *FormationMap {
	return &FormationMap{
		m: make(map[string]*formationS),
	}
}

func (fm *FormationMap) get(formationID string) *formationS {
	fm.l.RLock()
	defer fm.l.RUnlock()
	return fm.m[formationID]
}

// PutState ...
func (fm *FormationMap) PutState(formationID, key string, value interface{}) {
	fm.l.Lock()
	defer fm.l.Unlock()

	formation, exists := fm.m[formationID]

	if exists {
		formation.state[key] = value
	} else {
		formation = &formationS{
			state:   stateMap{key: value},
			devices: make(deviceStateMap),
		}

		fm.m[formationID] = formation
	}
}

// GetState ...
func (fm *FormationMap) GetState(formationID, key string) interface{} {
	fm.l.RLock()
	defer fm.l.RUnlock()

	formation, exists := fm.m[formationID]

	if !exists {
		return nil
	}

	return formation.state[key]
}

// PutDeviceState ...
func (fm *FormationMap) PutDeviceState(formationID, deviceName, key string, value interface{}) {
	fm.l.Lock()
	defer fm.l.Unlock()

	formation, fExists := fm.m[formationID]

	if !fExists {
		formation = &formationS{make(stateMap), make(deviceStateMap)}
		fm.m[formationID] = formation
	}

	deviceState, dExists := formation.devices[deviceName]
	if !dExists {
		deviceState = make(stateMap)
		formation.devices[deviceName] = deviceState
	}

	deviceState[key] = value
}

// GetDeviceState ...
func (fm *FormationMap) GetDeviceState(formationID, deviceName, key string) interface{} {
	fm.l.RLock()
	defer fm.l.RUnlock()

	formation, fExists := fm.m[formationID]
	if !fExists {
		return nil
	}

	deviceState, dExists := formation.devices[deviceName]
	if !dExists {
		return nil
	}

	return deviceState[key]
}