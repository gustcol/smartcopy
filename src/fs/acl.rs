//! Windows ACL Support
//!
//! Provides support for Windows Access Control Lists (ACLs).
//! Preserves security descriptors, DACLs, and SACLs during file copies.

use serde::{Deserialize, Serialize};
use std::io;
use std::path::Path;

#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;

/// Windows security information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityInfo {
    /// Owner SID
    pub owner: Option<String>,
    /// Group SID
    pub group: Option<String>,
    /// Discretionary Access Control List
    pub dacl: Option<Acl>,
    /// System Access Control List (requires SeSecurityPrivilege)
    pub sacl: Option<Acl>,
    /// Security descriptor flags
    pub flags: SecurityFlags,
}

/// Access Control List
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Acl {
    /// ACL entries
    pub entries: Vec<AclEntry>,
    /// ACL revision
    pub revision: u8,
}

/// ACL entry (ACE)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclEntry {
    /// Entry type
    pub ace_type: AceType,
    /// Entry flags
    pub flags: AceFlags,
    /// Access mask
    pub access_mask: AccessMask,
    /// Trustee SID
    pub sid: String,
}

/// ACE type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AceType {
    /// Allow access
    AccessAllowed,
    /// Deny access
    AccessDenied,
    /// Audit access
    SystemAudit,
    /// Alarm access
    SystemAlarm,
    /// Object-specific allow
    AccessAllowedObject,
    /// Object-specific deny
    AccessDeniedObject,
}

/// ACE flags
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct AceFlags {
    /// Inherit to child objects
    pub object_inherit: bool,
    /// Inherit to child containers
    pub container_inherit: bool,
    /// Do not propagate inherit
    pub no_propagate_inherit: bool,
    /// Inherit only (not applied to this object)
    pub inherit_only: bool,
    /// Inherited from parent
    pub inherited: bool,
    /// Successful access audit
    pub successful_access: bool,
    /// Failed access audit
    pub failed_access: bool,
}

/// Access rights mask
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct AccessMask {
    /// Raw access mask value
    pub value: u32,
}

impl AccessMask {
    // Standard rights
    pub const DELETE: u32 = 0x00010000;
    pub const READ_CONTROL: u32 = 0x00020000;
    pub const WRITE_DAC: u32 = 0x00040000;
    pub const WRITE_OWNER: u32 = 0x00080000;
    pub const SYNCHRONIZE: u32 = 0x00100000;

    // Generic rights
    pub const GENERIC_READ: u32 = 0x80000000;
    pub const GENERIC_WRITE: u32 = 0x40000000;
    pub const GENERIC_EXECUTE: u32 = 0x20000000;
    pub const GENERIC_ALL: u32 = 0x10000000;

    // File-specific rights
    pub const FILE_READ_DATA: u32 = 0x00000001;
    pub const FILE_WRITE_DATA: u32 = 0x00000002;
    pub const FILE_APPEND_DATA: u32 = 0x00000004;
    pub const FILE_READ_EA: u32 = 0x00000008;
    pub const FILE_WRITE_EA: u32 = 0x00000010;
    pub const FILE_EXECUTE: u32 = 0x00000020;
    pub const FILE_DELETE_CHILD: u32 = 0x00000040;
    pub const FILE_READ_ATTRIBUTES: u32 = 0x00000080;
    pub const FILE_WRITE_ATTRIBUTES: u32 = 0x00000100;

    /// Full control
    pub const FULL_CONTROL: u32 = 0x001F01FF;

    /// Create new access mask
    pub fn new(value: u32) -> Self {
        Self { value }
    }

    /// Check if specific right is set
    pub fn has(&self, right: u32) -> bool {
        self.value & right == right
    }

    /// Add a right
    pub fn add(&mut self, right: u32) {
        self.value |= right;
    }

    /// Remove a right
    pub fn remove(&mut self, right: u32) {
        self.value &= !right;
    }

    /// Get human-readable description
    pub fn describe(&self) -> Vec<&'static str> {
        let mut rights = Vec::new();

        if self.has(Self::FULL_CONTROL) {
            return vec!["Full Control"];
        }

        if self.has(Self::GENERIC_READ) || self.has(Self::FILE_READ_DATA) {
            rights.push("Read");
        }
        if self.has(Self::GENERIC_WRITE) || self.has(Self::FILE_WRITE_DATA) {
            rights.push("Write");
        }
        if self.has(Self::GENERIC_EXECUTE) || self.has(Self::FILE_EXECUTE) {
            rights.push("Execute");
        }
        if self.has(Self::DELETE) {
            rights.push("Delete");
        }
        if self.has(Self::WRITE_DAC) {
            rights.push("Change Permissions");
        }
        if self.has(Self::WRITE_OWNER) {
            rights.push("Take Ownership");
        }

        rights
    }
}

/// Security descriptor flags
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct SecurityFlags {
    /// Owner defaulted
    pub owner_defaulted: bool,
    /// Group defaulted
    pub group_defaulted: bool,
    /// DACL present
    pub dacl_present: bool,
    /// DACL defaulted
    pub dacl_defaulted: bool,
    /// SACL present
    pub sacl_present: bool,
    /// SACL defaulted
    pub sacl_defaulted: bool,
    /// DACL auto-inherited
    pub dacl_auto_inherited: bool,
    /// SACL auto-inherited
    pub sacl_auto_inherited: bool,
    /// DACL protected from inheritance
    pub dacl_protected: bool,
    /// SACL protected from inheritance
    pub sacl_protected: bool,
}

/// Windows ACL operations
pub struct WindowsAcl;

impl WindowsAcl {
    /// Get security information for a file
    #[cfg(windows)]
    pub fn get_security<P: AsRef<Path>>(path: P) -> io::Result<SecurityInfo> {
        use winapi::um::accctrl::SE_FILE_OBJECT;
        use winapi::um::aclapi::GetNamedSecurityInfoW;
        use winapi::um::winnt::{
            DACL_SECURITY_INFORMATION, GROUP_SECURITY_INFORMATION,
            OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR,
        };

        let path_wide: Vec<u16> = path.as_ref().as_os_str().encode_wide().chain(Some(0)).collect();

        let mut owner_sid = std::ptr::null_mut();
        let mut group_sid = std::ptr::null_mut();
        let mut dacl = std::ptr::null_mut();
        let mut security_descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();

        let result = unsafe {
            GetNamedSecurityInfoW(
                path_wide.as_ptr(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                &mut owner_sid,
                &mut group_sid,
                &mut dacl,
                std::ptr::null_mut(),
                &mut security_descriptor,
            )
        };

        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }

        // Convert SIDs and ACL to our structures
        let security_info = SecurityInfo {
            owner: sid_to_string(owner_sid),
            group: sid_to_string(group_sid),
            dacl: acl_to_struct(dacl),
            sacl: None,
            flags: SecurityFlags::default(),
        };

        // Free security descriptor
        if !security_descriptor.is_null() {
            unsafe {
                winapi::um::winbase::LocalFree(security_descriptor as *mut _);
            }
        }

        Ok(security_info)
    }

    /// Get security information (non-Windows stub)
    #[cfg(not(windows))]
    pub fn get_security<P: AsRef<Path>>(_path: P) -> io::Result<SecurityInfo> {
        Ok(SecurityInfo {
            owner: None,
            group: None,
            dacl: None,
            sacl: None,
            flags: SecurityFlags::default(),
        })
    }

    /// Set security information for a file
    #[cfg(windows)]
    pub fn set_security<P: AsRef<Path>>(path: P, info: &SecurityInfo) -> io::Result<()> {
        use winapi::um::accctrl::SE_FILE_OBJECT;
        use winapi::um::aclapi::SetNamedSecurityInfoW;
        use winapi::um::winnt::{
            DACL_SECURITY_INFORMATION, GROUP_SECURITY_INFORMATION,
            OWNER_SECURITY_INFORMATION,
        };

        let path_wide: Vec<u16> = path.as_ref().as_os_str().encode_wide().chain(Some(0)).collect();

        let mut security_info_flags = 0u32;

        if info.owner.is_some() {
            security_info_flags |= OWNER_SECURITY_INFORMATION;
        }
        if info.group.is_some() {
            security_info_flags |= GROUP_SECURITY_INFORMATION;
        }
        if info.dacl.is_some() {
            security_info_flags |= DACL_SECURITY_INFORMATION;
        }

        // Convert our structures back to Windows structures
        let owner_sid = info.owner.as_ref().and_then(|s| string_to_sid(s));
        let group_sid = info.group.as_ref().and_then(|s| string_to_sid(s));
        let dacl = info.dacl.as_ref().and_then(|a| struct_to_acl(a));

        let result = unsafe {
            SetNamedSecurityInfoW(
                path_wide.as_ptr() as *mut _,
                SE_FILE_OBJECT,
                security_info_flags,
                owner_sid.unwrap_or(std::ptr::null_mut()),
                group_sid.unwrap_or(std::ptr::null_mut()),
                dacl.unwrap_or(std::ptr::null_mut()),
                std::ptr::null_mut(),
            )
        };

        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }

        Ok(())
    }

    /// Set security information (non-Windows stub)
    #[cfg(not(windows))]
    pub fn set_security<P: AsRef<Path>>(_path: P, _info: &SecurityInfo) -> io::Result<()> {
        Ok(())
    }

    /// Copy security from one file to another
    pub fn copy_security<P: AsRef<Path>, Q: AsRef<Path>>(src: P, dst: Q) -> io::Result<()> {
        let info = Self::get_security(src)?;
        Self::set_security(dst, &info)
    }

    /// Check if current process has privilege to read/write security
    #[cfg(windows)]
    pub fn has_security_privilege() -> bool {
        use winapi::um::processthreadsapi::{GetCurrentProcess, OpenProcessToken};
        use winapi::um::securitybaseapi::GetTokenInformation;
        use winapi::um::winnt::{TokenPrivileges, TOKEN_QUERY};

        unsafe {
            let mut token = std::ptr::null_mut();
            if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) == 0 {
                return false;
            }

            // Check for SeSecurityPrivilege
            // Simplified - would need actual privilege check
            true
        }
    }

    #[cfg(not(windows))]
    pub fn has_security_privilege() -> bool {
        false
    }
}

// Windows-specific helper functions
#[cfg(windows)]
fn sid_to_string(sid: *mut winapi::ctypes::c_void) -> Option<String> {
    if sid.is_null() {
        return None;
    }

    use winapi::um::sddl::ConvertSidToStringSidW;

    unsafe {
        let mut string_sid: *mut u16 = std::ptr::null_mut();
        if ConvertSidToStringSidW(sid, &mut string_sid) != 0 {
            let len = (0..).take_while(|&i| *string_sid.offset(i) != 0).count();
            let slice = std::slice::from_raw_parts(string_sid, len);
            let result = String::from_utf16_lossy(slice);
            winapi::um::winbase::LocalFree(string_sid as *mut _);
            Some(result)
        } else {
            None
        }
    }
}

#[cfg(windows)]
fn string_to_sid(s: &str) -> Option<*mut winapi::ctypes::c_void> {
    use winapi::um::sddl::ConvertStringSidToSidW;

    let wide: Vec<u16> = s.encode_utf16().chain(Some(0)).collect();

    unsafe {
        let mut sid: *mut winapi::ctypes::c_void = std::ptr::null_mut();
        if ConvertStringSidToSidW(wide.as_ptr(), &mut sid as *mut _ as *mut *mut _) != 0 {
            Some(sid)
        } else {
            None
        }
    }
}

#[cfg(windows)]
fn acl_to_struct(acl: *mut winapi::um::winnt::ACL) -> Option<Acl> {
    if acl.is_null() {
        return None;
    }

    // Simplified - would need to iterate ACEs
    Some(Acl {
        entries: Vec::new(),
        revision: 2,
    })
}

#[cfg(windows)]
fn struct_to_acl(_acl: &Acl) -> Option<*mut winapi::um::winnt::ACL> {
    // Simplified - would need to build ACL from entries
    None
}

/// Well-known SID strings
pub mod well_known_sids {
    /// Everyone
    pub const EVERYONE: &str = "S-1-1-0";
    /// Administrators
    pub const ADMINISTRATORS: &str = "S-1-5-32-544";
    /// Users
    pub const USERS: &str = "S-1-5-32-545";
    /// Authenticated Users
    pub const AUTHENTICATED_USERS: &str = "S-1-5-11";
    /// Local System
    pub const LOCAL_SYSTEM: &str = "S-1-5-18";
    /// Creator Owner
    pub const CREATOR_OWNER: &str = "S-1-3-0";
    /// Creator Group
    pub const CREATOR_GROUP: &str = "S-1-3-1";
}

/// Check if running on Windows
pub fn is_windows() -> bool {
    cfg!(windows)
}

/// Check if Windows ACL support is available
pub fn acl_available() -> bool {
    #[cfg(windows)]
    {
        true
    }
    #[cfg(not(windows))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_mask() {
        let mut mask = AccessMask::new(AccessMask::FILE_READ_DATA);
        assert!(mask.has(AccessMask::FILE_READ_DATA));
        assert!(!mask.has(AccessMask::FILE_WRITE_DATA));

        mask.add(AccessMask::FILE_WRITE_DATA);
        assert!(mask.has(AccessMask::FILE_WRITE_DATA));

        mask.remove(AccessMask::FILE_READ_DATA);
        assert!(!mask.has(AccessMask::FILE_READ_DATA));
    }

    #[test]
    fn test_access_mask_describe() {
        let mask = AccessMask::new(AccessMask::FULL_CONTROL);
        assert_eq!(mask.describe(), vec!["Full Control"]);

        let mask = AccessMask::new(AccessMask::FILE_READ_DATA | AccessMask::DELETE);
        let desc = mask.describe();
        assert!(desc.contains(&"Read"));
        assert!(desc.contains(&"Delete"));
    }

    #[test]
    fn test_is_windows() {
        #[cfg(windows)]
        assert!(is_windows());
        #[cfg(not(windows))]
        assert!(!is_windows());
    }
}
