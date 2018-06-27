package hello;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Customer {

	
    @Override
	public String toString() {
		return "Customer [id=" + id + ", name=" + name + ", address=" + address + "]";
	}


	public Customer(@JsonProperty("id") String id, @JsonProperty("name") String name, @JsonProperty("address") String address) {
		this.id=id;
		this.name = name;
    	this.address = address;
	}


	private String id;
    private String name;
    private String address;


	public String getId() {
		return id;
	}


	public void setId(String id) {
		this.id = id;
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}


	public String getAddress() {
		return address;
	}


	public void setAddress(String address) {
		this.address = address;
	}
    
    
}
